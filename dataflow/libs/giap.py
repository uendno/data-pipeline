# Got It Analytics Platform

from requests import post, auth


def get_open_pn_events_of_user_list(uid_list,
                                    from_ts=0,
                                    to_ts=0,
                                    giap_es_username=None,
                                    giap_es_password=None,
                                    giap_es_url='https://d873955cbe044405b463edfbf85d674d.us-east-1.aws.found.io:9243',
                                    giap_es_index='analytics-photostudy-student-dev-events'
                                    ):
    # Analytics Platform uses timestamp in millisecond

    body = {
        "_source": [
            "uid",
            "$time",
            "$name",
            "campaign"
        ],
        "sort": [
            {
                "$time": {
                    "order": "desc"
                }
            }
        ],
        "query": {
            "bool": {
                "must": [
                    {
                        "exists": {
                            "field": "campaign"
                        }
                    },
                    {
                        "range": {
                            "$time": {
                                "gte": from_ts * 1000,
                                "lt": to_ts * 1000
                            }
                        }
                    },
                    {
                        "match": {
                            "$name": "Open Deep Link"
                        }
                    },
                    {
                        "terms": {
                            "uid": uid_list
                        }
                    }
                ]
            }
        }
    }

    response = post(giap_es_url + '/' + giap_es_index + '/_search',
                    json=body,
                    auth=auth.HTTPBasicAuth(giap_es_username, giap_es_password))
    response.raise_for_status()
    res_data = response.json()

    return res_data['hits']['hits']
