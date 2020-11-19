from __future__ import absolute_import

import argparse
from datetime import datetime, timedelta, time

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText, ReadFromBigQuery, Read
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import WindowedValue
from apache_beam.io.gcp.internal.clients import bigquery

from libs.giap import get_open_pn_events_of_user_list


class ParseRecentQuestionsCSV(beam.DoFn):
    def process(self, element):
        qid, uid, created = element.split(',')

        if qid == 'qid':
            return []

        return [(uid, {
            'qid': qid,
            'uid': uid,
            'created': created
        })]


class ParseRecentlyActiveUsersCSV(beam.DoFn):
    def process(self, element):
        uid, balance = element.split(',')

        if uid == 'uid':
            return []

        return [(uid, {
            'qid': uid,
            'balance': balance,
        })]


class CalculateAskEngagement(beam.DoFn):
    def __init__(self):
        self.window = GlobalWindow()
        self.batch = []
        self.engagement_range = 10
        self.from_ts = 0
        self.to_ts = 0
        self.giap_es_username = ''
        self.giap_es_password = ''
        self.giap_es_index = ''

    def process(self, group):
        self.batch.append(group)

    def finish_bundle(self):
        user_id_list = []
        new_engagement_records = []

        for group in self.batch:
            if len(group[1].get('engagements')) > 0:
                user_id_list.append(int(group[0]))

        events = []

        if len(user_id_list) > 0:
            # Get open PN events that happened from (from_ts -engagement_range) to (to_ts)
            # This make sure that if a question is created right after from_ts, we may
            # still get the open PN event that happened (engagement_range) days before that which leads to that question
            events = get_open_pn_events_of_user_list(user_id_list,
                                                     from_ts=self.from_ts - self.engagement_range * 24 * 60 * 60,
                                                     to_ts=self.to_ts,
                                                     giap_es_username=self.giap_es_username,
                                                     giap_es_password=self.giap_es_password,
                                                     giap_es_index=self.giap_es_index
                                                     )

        for group in self.batch:
            uid = int(group[0])
            questions = group[1].get('questions')
            engagements = group[1].get('engagements')
            users = group[1].get('users')

            if len(users) == 0 or len(questions) == 0 or len(engagements) == 0:
                continue

            user = users[0]

            user_events = [event for event in events if event['_source']['uid'] == uid]

            for question in questions:
                question_created = int(question.get('created'))

                for engagement in engagements:
                    send_push_noti_time = int(engagement.get('send_push_noti_time'))
                    question_created_date = datetime.fromtimestamp(question_created)
                    n_days_ago_date = question_created_date - timedelta(days=self.engagement_range)
                    n_days_ago_ts = n_days_ago_date.timestamp()

                    pn_campaign = None

                    for event in user_events:
                        # Find the last event which happens after send_push_noti_time and before question_created
                        if send_push_noti_time * 1000 < event['_source']['$time'] < question_created * 1000:
                            pn_campaign = event['_source'].get('campaign')
                            break

                    if question_created > send_push_noti_time > n_days_ago_ts:
                        new_engagement_record = {
                            'created': int(datetime.now().timestamp()),
                            'action': 'ask',
                            'question_id': question['qid'],
                            'uid': uid,
                            'type': engagement.get('type'),
                            'inactive_days': engagement.get('inactive_days'),
                            'balance': user.get('balance', 0),
                            'grade': engagement.get('grade'),
                            'send_push_noti_time': engagement.get('send_push_noti_time'),
                            'action_time': question['created'],
                            'campaign': pn_campaign
                        }

                        new_engagement_records.append(new_engagement_record)

        for record in new_engagement_records:
            yield WindowedValue(
                value=record,
                timestamp=0,
                windows=[self.window],
            )


def get_recent_questions(p, file):
    return (
            p
            | "Read recent questions" >> ReadFromText(file)
            | "Parse recent questions" >> beam.ParDo(ParseRecentQuestionsCSV())
    )


def get_recently_active_users(p, file):
    return (
            p
            | "Read recently active users" >> ReadFromText(file)
            | "Parse recently active users" >> beam.ParDo(ParseRecentlyActiveUsersCSV())
    )


def get_latest_engagements(p, from_ts=0, to_ts=0, engagement_range=10):
    # Get "send" engagements that happened from (from_ts -engagement_range) to (to_ts)
    # This make sure that if a question is created right after from_ts, we may
    # still get the "send" engagement that happened (engagement_range) days before that which lead to the question

    return (
            p
            | 'Read recent "send" engagement' >> ReadFromBigQuery(project='gotit-analytics',
                                                                  query=f'SELECT * FROM study_pn_campaign.engagement '
                                                                        f'WHERE action = "send" '
                                                                        f'AND send_push_noti_time >= {from_ts - engagement_range * 24 * 60 * 60} '
                                                                        f'AND send_push_noti_time < {to_ts}'
                                                                  )
            | 'Parse recent "send" engagement' >> beam.Map(lambda engagement: (str(engagement['uid']), engagement))
    )

    # return (
    #         p | beam.Create([
    #     ('1', {
    #         'uid': 1,
    #         'send_push_noti_time': 1605481200,
    #         'type': 'test',
    #         'inactive_days': 0
    #     })
    # ]))


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--questions',
        dest='questions',
        required=True,
        help='Questions file.')
    parser.add_argument(
        '--users',
        dest='users',
        required=True,
        help='Users file.')
    parser.add_argument(
        '--from-ts',
        dest='from_ts',
        required=True,
        type=int,
        help='Start of the time range.')
    parser.add_argument(
        '--to-ts',
        dest='to_ts',
        required=True,
        type=int,
        help='End of the time range.')
    parser.add_argument(
        '--engagement-range',
        dest='engagement_range',
        default=10,
        type=int,
        help='Maximum number of days from first step to the last step of an engagement.')
    parser.add_argument(
        '--giap-es-index',
        dest='giap_es_index',
        required=True,
        help='GIAP ES index.')
    parser.add_argument(
        '--giap-es-username',
        dest='giap_es_username',
        required=True,
        help='GIAP ES username.')
    parser.add_argument(
        '--giap-es-password',
        dest='giap_es_password',
        required=True,
        help='GIAP ES password.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        recent_questions = get_recent_questions(p, known_args.questions)

        recently_active_users = get_recently_active_users(p, known_args.users)

        latest_engagements = get_latest_engagements(p,
                                                    from_ts=known_args.from_ts,
                                                    to_ts=known_args.to_ts,
                                                    engagement_range=known_args.engagement_range
                                                    )

        question_engagement_pairs = ({
            'questions': recent_questions,
            'engagements': latest_engagements,
            'users': recently_active_users
        }) | "Group by uid" >> beam.CoGroupByKey()

        calculateAskEngagement = CalculateAskEngagement()
        calculateAskEngagement.engagement_range = known_args.engagement_range
        calculateAskEngagement.from_ts = known_args.from_ts
        calculateAskEngagement.to_ts = known_args.to_ts
        calculateAskEngagement.giap_es_index = known_args.giap_es_index
        calculateAskEngagement.giap_es_username = known_args.giap_es_username
        calculateAskEngagement.giap_es_password = known_args.giap_es_password

        engagement_table_spec = bigquery.TableReference(
            projectId='gotit-analytics',
            datasetId='study_pn_campaign',
            tableId='engagement')

        new_engagements = (question_engagement_pairs
                           | "Calculate 'ask' engagements" >> beam.ParDo(calculateAskEngagement)
                           | 'Write result to BQ' >> beam.io.WriteToBigQuery(engagement_table_spec,
                                                                             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                                             ))

        # new_engagements = (question_engagement_pairs
        #                    | beam.ParDo(process_recently_engaged_askers_fn)
        #                    | WriteToText(known_args.output)
        #                    )

        # new_engagements | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
    run()
