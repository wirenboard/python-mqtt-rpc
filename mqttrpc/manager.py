import json
import logging

from jsonrpc.exceptions import (
    JSONRPCDispatchException,
    JSONRPCInvalidParams,
    JSONRPCInvalidRequest,
    JSONRPCInvalidRequestException,
    JSONRPCMethodNotFound,
    JSONRPCParseError,
    JSONRPCServerError,
)
from jsonrpc.utils import is_invalid_params

from .protocol import MQTTRPC10Request, MQTTRPC10Response

logger = logging.getLogger(__name__)


class MQTTRPCResponseManager(object):
    """MQTT-RPC response manager.

    Method brings syntactic sugar into library. Given dispatcher it handles
    request (both single and batch) and handles errors.
    Request could be handled in parallel, it is server responsibility.

    :param str request_str: json string. Will be converted into
        MQTTRPC10Request

    :param dict dispather: dict<function_name:function>.

    """

    @classmethod
    def _prepare_request(cls, request_str):
        if isinstance(request_str, bytes):
            request_str = request_str.decode("utf-8")
        try:
            json.loads(request_str)
        except (TypeError, ValueError):
            return None, MQTTRPC10Response(error=JSONRPCParseError()._data)
        try:
            request = MQTTRPC10Request.from_json(request_str)
        except JSONRPCInvalidRequestException:
            return None, MQTTRPC10Response(error=JSONRPCInvalidRequest()._data)

        return request, None

    @classmethod
    def handle(cls, request_str, service_id, method_id, dispatcher):
        request, erroneous_response = cls._prepare_request(request_str)
        if request:
            return cls.handle_request(request, service_id, method_id, dispatcher)
        else:
            return erroneous_response

    @classmethod
    def _process_exception(cls, request, method, e):
        data = {
            "type": e.__class__.__name__,
            "args": e.args,
            "message": str(e),
        }

        if isinstance(e, JSONRPCDispatchException):
            return MQTTRPC10Response(_id=request._id, error=e.error._data)
        elif isinstance(e, TypeError) and is_invalid_params(method, *request.args, **request.kwargs):
            return MQTTRPC10Response(_id=request._id, error=JSONRPCInvalidParams(data=data)._data)
        else:
            logger.exception("API Exception: {0}".format(data))
            return MQTTRPC10Response(_id=request._id, error=JSONRPCServerError(data=data)._data)

    @classmethod
    def handle_request(cls, request, service_id, method_id, dispatcher):
        """Handle request data.

        At this moment request has correct jsonrpc format.

        :param dict request: data parsed from request_str.
        :param jsonrpc.dispatcher.Dispatcher dispatcher:

        .. versionadded: 1.8.0

        """
        try:
            method = dispatcher[(service_id, method_id)]
        except KeyError:
            output = MQTTRPC10Response(_id=request._id, error=JSONRPCMethodNotFound()._data)
        else:
            try:
                result = method(*request.args, **request.kwargs)
            except Exception as e:
                output = cls._process_exception(request, method, e)
            else:
                output = MQTTRPC10Response(_id=request._id, result=result)
        finally:
            if not request.is_notification:
                return output
            else:
                return []


class AMQTTRPCResponseManager(MQTTRPCResponseManager):
    """
    asyncio-compatible version of MQTTRPCResponseManager
    """

    @classmethod
    async def handle(cls, request_str, service_id, method_id, dispatcher):
        request, erroneous_response = cls._prepare_request(request_str)
        if request:
            return await cls.handle_request(request, service_id, method_id, dispatcher)
        else:
            return erroneous_response

    @classmethod
    async def handle_request(cls, request, service_id, method_id, dispatcher):
        try:
            method = dispatcher[(service_id, method_id)]
        except KeyError:
            output = MQTTRPC10Response(_id=request._id, error=JSONRPCMethodNotFound()._data)
        else:
            try:
                result = await method(*request.args, **request.kwargs)
            except Exception as e:
                output = cls._process_exception(request, method, e)
            else:
                output = MQTTRPC10Response(_id=request._id, result=result)
        finally:
            if not request.is_notification:
                return output
            else:
                return []
