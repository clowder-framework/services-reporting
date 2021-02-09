FROM clowder/pyclowder:onbuild

RUN pip install pymongo

COPY *.py  /home/monitor/

ENV RABBITMQ_EXCHANGE="clowder.metrics" \
    RABBITMQ_KEY="event.sink" \
    RABBITMQ_QUEUE="event.sink"

CMD python /home/monitor/activity-monitor.py
