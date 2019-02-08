import boto3

CLASSIFIER_CLASSIFICATION = 'my-custom-log-format'
CLASSIFIER_NAME = 'my-custom-log-format'
CLASSIFIER_GROKPATTERN = '%{MYLOGFORMAT}'
MYLOGFORMAT = "MYLOGFORMAT %{IPORHOST:clientip} - %{USER:ident} %{USER:auth}"

CRAWLER_NAME = 'my-custom-log-crawler'
CRAWLER_IAM_ROLE = 'service-role/AWSGlueServiceRole-js-logs-example'
CRAWLER_DATABASE_NAME = 'my-logs'
CRAWLER_S3_TARGET_PATH = 's3://js-example-logs/source/custom'

client = boto3.client('glue')

def list_classifiers():
    response = client.get_classifier(Name='my-custom-format')
    print('response: %s' % response)

def check_if_classifier_exists():
    try:
        response = client.get_classifier(Name=CLASSIFIER_NAME)
        print('response %s' % response)
        if response:
            return True
    except Exception:
        print('not found')
        return False


def create_classifier():
    client.create_classifier(
        GrokClassifier={
            'Classification': CLASSIFIER_CLASSIFICATION,
            'Name': CLASSIFIER_NAME,
            'GrokPattern': CLASSIFIER_GROKPATTERN,
            'CustomPatterns': MYLOGFORMAT
        }
    )

def update_classifier():
    client.update_classifier(
        GrokClassifier={
            'Classification': CLASSIFIER_CLASSIFICATION,
            'Name': CLASSIFIER_NAME,
            'GrokPattern': CLASSIFIER_GROKPATTERN,
            'CustomPatterns': MYLOGFORMAT
        }
    )

def create_or_update_classifier():
    if check_if_classifier_exists():
        update_classifier()
    else:
        create_classifier()


def create_crawler():
    try:
        client.get_crawler(Name=CRAWLER_NAME)
        print("crawler already exist, deleting it...")
        client.delete_crawler(Name=CRAWLER_NAME)
    except Exception:
        print("crawler doesn't exist")
    print('creating crawler')
    client.create_crawler(
        Name=CRAWLER_NAME,
        Role=CRAWLER_IAM_ROLE,
        DatabaseName=CRAWLER_DATABASE_NAME,
        Targets={
            'S3Targets': [
                {
                    'Path': CRAWLER_S3_TARGET_PATH,
                    'Exclusions': []
                }
            ]
        },
        Classifiers=[
            CLASSIFIER_NAME
        ]
    )

def start_crawler():
    print('starting crawler')
    client.start_crawler(Name=CRAWLER_NAME)

def main():
    list_classifiers()
    create_or_update_classifier()
    create_crawler()
    start_crawler()


if __name__ == '__main__':
    main()
