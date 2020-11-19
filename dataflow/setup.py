import setuptools

setuptools.setup(
    name='study-pn-campaign-dataflow',
    version='1.0.0',
    install_requires=[
        'apache-beam[gcp]==2.25.0',
        'requests'
    ],
    packages=setuptools.find_packages(),
)
