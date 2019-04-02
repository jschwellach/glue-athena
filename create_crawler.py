# Free Public License 1.0.0
# Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby granted.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#

import boto3
import json

# change the variables according to your needs

CLASSIFIER_CLASSIFICATION = 'my-custom-log-format'
CLASSIFIER_NAME = 'my-custom-log-format'
CLASSIFIER_GROKPATTERN = '%{MYLOGFORMAT}'
CRAWLER_NAME = 'my-custom-log-crawler'
# Change it to your service-role
CRAWLER_IAM_ROLE = 'service-role/AWSGlueServiceRole-js-logs-example'
# Change it to your database in glue/athena
CRAWLER_DATABASE_NAME = 'my-logs'
# Change it to your S3 bucket
CRAWLER_S3_TARGET_PATH = 's3://js-example-logs/source/custom'

# Custom log format
MYLOGFORMAT = '''
MYLOGFORMAT %{IPORHOST:clientip} - %{USER:ident} %{USER:auth} \[%{TIMESTAMP_ISO8601:timestamp}\] "(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|%{DATA:rawrequest})" %{NUMBER:response} (?:%{Bytes:bytes=%{NUMBER}|-}) %{QS:referrer} %{QS:agent}
'''

SECONDFORMAT = '''
SECONDFORMAT %{GREEDYDATA:data}
'''

client = boto3.client('glue')

def list_classifiers():
    try:
        response = client.get_classifier(Name='my-custom-format')
        print('response: %s' % response)
    except Exception:
        return ""

def check_if_classifier_exists(name):
    try:
        response = client.get_classifier(Name=name)
        print('response %s' % response)
        if response:
            return True
    except Exception:
        print('not found')
        return False


def create_classifier():
    if check_if_classifier_exists(CLASSIFIER_NAME):
        params = {
            'CLASSIFICATION': CLASSIFIER_CLASSIFICATION,
            'NAME': CLASSIFIER_NAME,
            'GROKPATTERN': CLASSIFIER_GROKPATTERN,
            'LOGFORMAT': MYLOGFORMAT
        }
        update_classifier(params)
    else:
        client.create_classifier(
            GrokClassifier={
                'Classification': CLASSIFIER_CLASSIFICATION,
                'Name': CLASSIFIER_NAME,
                'GrokPattern': CLASSIFIER_GROKPATTERN,
                'CustomPatterns': MYLOGFORMAT
            }
        )
    if check_if_classifier_exists('second-format'):
        params = {
            'CLASSIFICATION': 'second-format',
            'NAME': 'second-format',
            'GROKPATTERN': '%{SECONDFORMAT}',
            'LOGFORMAT': SECONDFORMAT
        }
        update_classifier(params)
    else:
        client.create_classifier(
            GrokClassifier={
                'Classification': 'second-format',
                'Name': 'second-format',
                'GrokPattern': '%{SECONDFORMAT}',
                'CustomPatterns': SECONDFORMAT
            }
        )

def update_classifier(params):
    client.update_classifier(
        GrokClassifier={
            'Classification': params['CLASSIFICATION'],
            'Name': params['NAME'],
            'GrokPattern': params['GROKPATTERN'],
            'CustomPatterns': params['LOGFORMAT']
        }
    )


def create_crawler():
    try:
        response = client.get_crawler(Name=CRAWLER_NAME)
        print("response %s" % response)
        print("crawler already exist, deleting it...")
        client.delete_crawler(Name=CRAWLER_NAME)
    except Exception:
        print("crawler doesn't exist")
    print('creating crawler')
    configuration = {
        "Version": 1.0,
        "Grouping": {
            "TableGroupingPolicy": "CombineCompatibleSchemas"
        }
    }
    client.create_crawler(
        Name=CRAWLER_NAME,
        Role=CRAWLER_IAM_ROLE,
        DatabaseName=CRAWLER_DATABASE_NAME,
        Targets={
            'S3Targets': [
                {
                    'Path': CRAWLER_S3_TARGET_PATH,
                    'Exclusions': [
                        "**/*.err.gz"
                    ]
                }
            ]
        },
        Classifiers=[
            CLASSIFIER_NAME,
            'second-format'
        ],
        Configuration=json.dumps(configuration)
    )

def start_crawler():
    print('starting crawler')
    client.start_crawler(Name=CRAWLER_NAME)

def main():
    list_classifiers()
    create_classifier()
    create_crawler()
    start_crawler()


if __name__ == '__main__':
    main()
