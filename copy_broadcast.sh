python run_senders_from_config.py \
    --zmq-sub \
    --zmq-sub-endpoint tcp://localhost:5876 \
    --zmq-sub-topic "" \
    --forward-pub \
    --forward-bind "tcp://192.168.5.102:5877" \
    --forward-topic ""