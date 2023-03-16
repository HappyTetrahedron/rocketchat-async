import hashlib
import time


class RealtimeRequest:
    """Method call or subscription in the RocketChat realtime API."""
    _max_id = 0

    @staticmethod
    def _get_new_id():
        RealtimeRequest._max_id += 1
        return f'{RealtimeRequest._max_id}'


class Connect(RealtimeRequest):
    """Initialize the connection."""

    REQUEST_MSG = {
        'msg': 'connect',
        'version': '1',
        'support': ['1'],
    }

    @classmethod
    async def call(cls, dispatcher):
        await dispatcher.call_method(cls.REQUEST_MSG)


class Login(RealtimeRequest):
    """Log in to the service."""

    @staticmethod
    def _get_request_msg(msg_id, args):
        msg = {
            "msg": "method",
            "method": "login",
            "id": msg_id,
        }

        if len(args) == 1:
            msg["params"] = [{
                "resume": args[0]
            }]
        else:
            pwd_digest = hashlib.sha256(args[1].encode()).hexdigest()
            msg["params"] = [
                {
                    "user": {"username": args[0]},
                    "password": {
                        "digest": pwd_digest,
                        "algorithm": "sha-256"
                    }
                }
            ]
        return msg

    @staticmethod
    def _parse(response):
        return response['result']['id']

    @classmethod
    async def call(cls, dispatcher, args):
        msg_id = cls._get_new_id()

        msg = cls._get_request_msg(msg_id, args)
        response = await dispatcher.call_method(msg, msg_id)
        return cls._parse(response)


class GetChannels(RealtimeRequest):
    """Get a list of channels user is currently member of."""

    @staticmethod
    def _get_request_msg(msg_id):
        return {
            'msg': 'method',
            'method': 'rooms/get',
            'id': msg_id,
            'params': [],
        }

    @staticmethod
    def _parse(response):
        # Return channel IDs and channel types.
        return [(r['_id'], r['t']) for r in response['result']]

    @classmethod
    async def call(cls, dispatcher):
        msg_id = cls._get_new_id()
        msg = cls._get_request_msg(msg_id)
        response = await dispatcher.call_method(msg, msg_id)
        return cls._parse(response)


class SendMessage(RealtimeRequest):
    """Send a text message to a channel."""

    @staticmethod
    def _get_request_msg(msg_id, channel_id, msg_text, thread_id=None):
        id_seed = f'{msg_id}:{time.time()}'
        msg = {
            "msg": "method",
            "method": "sendMessage",
            "id": msg_id,
            "params": [
                {
                    "_id": hashlib.md5(id_seed.encode()).hexdigest()[:12],
                    "rid": channel_id,
                    "msg": msg_text
                }
            ]
        }
        if thread_id is not None:
            msg["params"][0]["tmid"] = thread_id
        return msg

    @classmethod
    async def call(cls, dispatcher, msg_text, channel_id, thread_id=None):
        msg_id = cls._get_new_id()
        msg = cls._get_request_msg(msg_id, channel_id, msg_text, thread_id)
        await dispatcher.call_method(msg, msg_id)


class SendReaction(RealtimeRequest):
    """Send a reaction to a specific message."""

    @staticmethod
    def _get_request_msg(msg_id, orig_msg_id, emoji):
        return {
            "msg": "method",
            "method": "setReaction",
            "id": msg_id,
            "params": [
                emoji,
                orig_msg_id,
            ]
        }

    @classmethod
    async def call(cls, dispatcher, orig_msg_id, emoji):
        msg_id = cls._get_new_id()
        msg = cls._get_request_msg(msg_id, orig_msg_id, emoji)
        await dispatcher.call_method(msg)


class SendTypingEvent(RealtimeRequest):
    """Send the `typing` event to a channel."""

    @staticmethod
    def _get_request_msg(msg_id, channel_id, username, is_typing):
        return {
            "msg": "method",
            "method": "stream-notify-room",
            "id": msg_id,
            "params": [
                f'{channel_id}/typing',
                username,
                is_typing
            ]
        }

    @classmethod
    async def call(cls, dispatcher, channel_id, username, is_typing):
        msg_id = cls._get_new_id()
        msg = cls._get_request_msg(msg_id, channel_id, username, is_typing)
        await dispatcher.call_method(msg, msg_id)


class SubscribeToChannelMessages(RealtimeRequest):
    """Subscribe to all messages in the given channel."""

    @staticmethod
    def _get_request_msg(msg_id, channel_id):
        return {
            "msg": "sub",
            "id": msg_id,
            "name": "stream-room-messages",
            "params": [
                channel_id,
                {
                    "useCollection": False,
                    "args": []
                }
            ]
        }

    @staticmethod
    def _wrap(callback):
        def fn(msg):
            message = msg['fields']['args'][0]  # TODO: This looks suspicious.
            msg_id = message['_id']
            channel_id = message['rid']
            sender_id = message['u']['_id']
            return callback(channel_id, sender_id, msg_id,
                            message)
        return fn

    @classmethod
    async def call(cls, dispatcher, channel_id, callback):
        # TODO: document the expected interface of the callback.
        msg_id = cls._get_new_id()
        msg = cls._get_request_msg(msg_id, channel_id)
        await dispatcher.create_subscription(msg, msg_id, cls._wrap(callback))
        return msg_id  # Return the ID to allow for later unsubscription.


class SubscribeToChannelChanges(RealtimeRequest):
    """Subscribe to all changes in channels."""

    @staticmethod
    def _get_request_msg(msg_id, user_id):
        return {
            "msg": "sub",
            "id": msg_id,
            "name": "stream-notify-user",
            "params": [
                f'{user_id}/rooms-changed',
                False
            ]
        }

    @staticmethod
    def _wrap(callback):
        def fn(msg):
            payload = msg['fields']['args']
            if payload[0] == 'removed':
                return  # Nothing else to do - channel has just been deleted.
            channel_id = payload[1]['_id']
            channel_type = payload[1]['t']
            return callback(channel_id, channel_type)
        return fn

    @classmethod
    async def call(cls, dispatcher, user_id, callback):
        # TODO: document the expected interface of the callback.
        msg_id = cls._get_new_id()
        msg = cls._get_request_msg(msg_id, user_id)
        await dispatcher.create_subscription(msg, msg_id, cls._wrap(callback))
        return msg_id  # Return the ID to allow for later unsubscription.


class Unsubscribe(RealtimeRequest):
    """Cancel a subscription"""

    @staticmethod
    def _get_request_msg(subscription_id):
        return {
            "msg": "unsub",
            "id": subscription_id,
        }

    @classmethod
    async def call(cls, dispatcher, subscription_id):
        msg = cls._get_request_msg(subscription_id)
        await dispatcher.call_method(msg)
