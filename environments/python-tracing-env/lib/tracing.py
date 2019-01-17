from jaeger_client import Config
from flask import request, make_response, g
from opentracing.propagation import Format
from opentracing.ext import tags as ext_tags
from uuid import UUID
import logging

logger = logging.getLogger("lib.tracing")
# Tracer is created globally and initialized only once because of
# following bug.
# https://github.com/jaegertpracing/jaeger-client-python/issues/50
# We won't be calling tracer.close()
tracer = None


def initialize_tracing(func):
    """
    Decorator which initializes the tracing related stuff.
    It creates tracer and a new span.
    Also extracts and injects the headers required for Jaeger tracing
    to work across multiple functions

    :param func: Function object
    :returns: decorated function
    """
    def inner():
        global tracer
        trace_id = None

        event_id = request.json.get('eventID', None)
        if event_id is not None:
            logger.info("processing eventID: %s", event_id)
            try:
                trace_id = _uuid_string_to_int(event_id)
            except ValueError as value_error:
                logger.warning("Error parsing eventID, random number will be \
used as trace_id: %s", value_error)
        else:
            logger.warning("eventID is not present in the JSON. Random number \
will be generated as trace_id")

        fission_func_name = request.headers.get("X-Fission-Function-Name",
                                                "name")
        span_name = fission_func_name + "-span"
        if tracer is None:
            tracer = _init_tracer(fission_func_name)
            logger.info("created new tracer: %s", tracer)
        span_ctx = tracer.extract(Format.HTTP_HEADERS, request.headers)
        logger.info("created new span_ctx: %s", span_ctx)
        response = None
        with tracer.start_span(span_name, child_of=span_ctx) as span:
            # set the eventID as trace_id if not a child span
            # and trace_id is parsed successfully
            if not span_ctx and trace_id:
                span.context.trace_id = trace_id
            span.set_tag("generated-by", "lib.tracing")
            span.set_tag(ext_tags.SAMPLING_PRIORITY, 1)
            logger.info("span value: %s", span)
            generated_headers = dict()
            tracer.inject(span, Format.HTTP_HEADERS, generated_headers)
            logger.info("generated_headers: %s", generated_headers)
            # User may want to set tags on span or use the generated_headers
            g.span = span
            g.generated_headers = generated_headers
            # User may return a None, string or object of response
            # Supported types:
            # http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response
            func_resp = func()
            logger.info("response from function: %s", func_resp)
            if func_resp is None:
                response = make_response()
            else:
                response = make_response(func_resp)
            for key, value in generated_headers.items():
                response.headers[key] = value
        logger.info("generated response data: %s", response.data)
        logger.info("generated response headers: \n%s", response.headers)
        return response
    return inner


def _init_tracer(service):
    """
    This takes a name of service and creates new tracer using
    jaeger_client.
    reporting_host is taken from environment variable
    JAEGER_AGENT_HOST
    reporting_port is taken from environment variable
    JAEGER_AGENT_PORT

    :param service: name of service (string)
    :returns: tracer object
    """
    client_config = Config(
        config={
            "sampler": {"type": "const", "param": 1},
            "logging": True,
            "generate_128bit_trace_id": True,
        },
        service_name=service,
    )
    return client_config.new_tracer()


def _uuid_string_to_int(uuid_string):
    """
    Converts the UUID string to integer number

    :param uuid_string: UUID in string format
    :returns: integer number
    """
    return UUID(uuid_string).int
