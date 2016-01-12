FROM ruby:2.1

RUN apt-get update && apt-get install -y haproxy

ADD . /tmp/synapse/
RUN cd /tmp/synapse && bundle install

CMD /etc/init.d/haproxy start && synapse -c /etc/synapse.conf.json
