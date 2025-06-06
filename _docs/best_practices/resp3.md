RESP 3 Features

valkey-py supports the RESP 3 standard. Practically, this means that client using RESP 3 will be faster and more performant as fewer type translations occur in the client. It also means new response types like doubles, true simple strings, maps, and booleans are available.
Connecting

Enabling RESP3 is no different than other connections in valkey-py. In all cases, the connection type must be extending by setting protocol=3. The following are some base examples illustrating how to enable a RESP 3 connection.

Connect with a standard connection, but specifying resp 3:

import valkey

r = valkey.Valkey(host='localhost', port=6379, protocol=3)

r.ping()

Or using the URL scheme:

import valkey

r = valkey.from_url("valkey://localhost:6379?protocol=3")

r.ping()

Connect with async, specifying resp 3:

import valkey.asyncio as valkey

r = valkey.Valkey(host='localhost', port=6379, protocol=3)

await r.ping()

The URL scheme with the async client

import valkey.asyncio as Valkey

r = valkey.from_url("valkey://localhost:6379?protocol=3")

await r.ping()

Connecting to an OSS Valkey Cluster with RESP 3

from valkey.cluster import ValkeyCluster, ClusterNode

r = ValkeyCluster(startup_nodes=[ClusterNode('localhost', 6379), ClusterNode('localhost', 6380)], protocol=3)

r.ping()

Push notifications

Push notifications are a way that valkey sends out of band data. The RESP 3 protocol includes a push type that allows our client to intercept these out of band messages. By default, clients will log simple messages, but valkey-py includes the ability to bring your own function processor.

This means that should you want to perform something, on a given push notification, you specify a function during the connection, as per this examples:

>> from valkey import Valkey
>>
>> def our_func(message):
>>    if message.find("This special thing happened"):
>>        raise IOError("This was the message: \n" + message)
>>
>> r = Valkey(protocol=3)
>> p = r.pubsub(push_handler_func=our_func)

In the example above, upon receipt of a push notification, rather than log the message, in the case where specific text occurs, an IOError is raised. This example, highlights how one could start implementing a customized message handler.