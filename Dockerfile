# DynamoDB EventStore (github.com/adbrowne/DynamoEventStore)
#
# VERSION               0.0.1

FROM      ubuntu
MAINTAINER Andrew Browne <brownie@brownie.com.au>

RUN apt-get update && apt-get install -y curl && apt-get clean

RUN mkdir /opt/dynamoEventStore && curl https://github.com/adbrowne/DynamoEventStore/releases/download/v0.0.1/web > /opt/dynamoEventStore/web

EXPOSE 3000

WORKDIR /opt/dynamoEventStore

CMD ./web
