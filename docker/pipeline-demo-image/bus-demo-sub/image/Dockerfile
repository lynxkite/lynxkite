FROM pipeline-demo

WORKDIR /

RUN echo "export KITE_PREFIX_DEFINITIONS=/tmp/prefix_definitions.txt" >> /tmp/kiterc

ADD prefix_definitions.txt /tmp/

RUN mkdir -p /tmp/lake/input_base
ADD bus-schedules-8-dedup.csv /tmp/lake/input_base/
ADD bus-stops.csv /tmp/lake/input_base/

CMD /tmp/start.sh
