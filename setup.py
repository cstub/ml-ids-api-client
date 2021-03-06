from distutils.core import setup

setup(
    name='ml-ids-api-client',
    version='0.1',
    description='API Client for the Machine learning based Intrusion Detection System',
    long_description='API Client for the Machine learning based Intrusion Detection System',
    classifiers=[
        'Programming Language :: Python :: 3',
    ],
    url='https://github.com/cstub/ml-ids-api-client',
    author='cstub',
    author_email='stumpf.christoph@gmail.com',
    license='MIT',
    packages=['ml_ids_api_client'],
    install_requires=[
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'ml_ids_rest_client=ml_ids_api_client.producer.rest_client:run_client',
            'ml_ids_attack_consumer=ml_ids_api_client.consumer.attack_notification_consumer:run_cli'
        ]
    }
)
