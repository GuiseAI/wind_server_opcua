import asyncio
import copy
import logging
from datetime import datetime
import time
from math import sin

import csv
from asyncua import ua, uamethod, Server
_logger = logging.getLogger(__name__)


class SubHandler(object):

    """
    Subscription Handler. To receive events from server for a subscription
    """

    def datachange_notification(self, node, val, data):
        _logger.warning("Python: New data change event %s %s", node, val)

    def event_notification(self, event):
        _logger.warning("Python: New event %s", event)


# method to be exposed through server
def func(parent, variant):
    ret = False
    if variant.Value % 2 == 0:
        ret = True
    return [ua.Variant(ret, ua.VariantType.Boolean)]



async def main():

    server = Server()
    await server.init()
    # server.disable_clock()  #for debuging
    # server.set_endpoint("opc.tcp://localhost:4840/freeopcua/server/")
    server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")
    server.set_server_name("Wind Turbine Server")
    # set all possible endpoint policies for clients to connect through
    server.set_security_policy(
        [
            ua.SecurityPolicyType.NoSecurity,
            ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
            ua.SecurityPolicyType.Basic256Sha256_Sign,
        ]
    )

    # setup our own namespace
    uri = "http://examples.freeopcua.github.io"
    idx = await server.register_namespace(uri)

    
    myobj = await server.nodes.objects.add_object(idx, "Wind Turbine")
    var_list = [
    ('Time_stamp_full', ua.VariantType.Double),
    ('TurbID', ua.VariantType.String),
    ('Wspd', ua.VariantType.Double),
    ('Wdir', ua.VariantType.Double),
    ('Etmp', ua.VariantType.Double),
    ('Itmp', ua.VariantType.Double),
    ('Ndir', ua.VariantType.Double),
    ('Pab1', ua.VariantType.Double),
    ('Pab2', ua.VariantType.Double),
    ('Pab3', ua.VariantType.Double),
    ('Prtv', ua.VariantType.Double),
    ('Patv', ua.VariantType.Double),
    ('zone_id', ua.VariantType.Double)]

    myvar_list = {}

    for var_name, var_type in var_list:
        if var_name == 'Time_stamp_full':
            myvar = await myobj.add_variable(idx, var_name, "temp")
        else:
            myvar = await myobj.add_variable(idx, var_name, 6.7)
        
        # await myvar.set_node_class(ua.NodeClass.Variable)  # Set the node class to Variable
        # await myvar.set_data_type(ua.NodeId(ua.ObjectIds.String))  # Set the data type (modify as needed)
        await myvar.set_writable(True)  # Set whether the variable is writable
        myvar_list[var_name] = myvar
    
    # for var in var_list:
    #     if var == 'Time_stamp_full':
    #         myvar = await myobj.add_variable(idx, var, "temp")
    #     else:
    #         myvar = await myobj.add_variable(idx, var, 6.7)

    #     myvar_list[var] = myvar
    
    # starting!
    async with server:
        # print("Available loggers are: ", logging.Logger.manager.loggerDict.keys())
        # enable following if you want to subscribe to nodes on server side
        # handler = SubHandler()
        # sub = await server.create_subscription(500, handler)
        # handle = await sub.subscribe_data_change(myvar)
        # trigger event, all subscribed clients wil receive it
        
        # write_attribute_value is a server side method which is faster than using write_value
        # but than methods has less checks

        while True:
            # Open the CSV file
            with open('turbine34_final.csv', mode='r') as file:
                    # Create a CSV DictReader object
                    csv_reader = csv.DictReader(file)

                    # Iterate through each row in the CSV as a dictionary
                    for row in csv_reader:
                        await asyncio.sleep(1)
                        # Each row is a dictionary with column names as keys
                        for var_name,var_type in var_list:
                            if var_name == 'Time_stamp_full':
                                val = row[var_name]
                            else:
                                val = float(row[var_name])
                            # print(val)
                            # print(myvar_list[var])

                            await server.write_attribute_value(myvar_list[var_name].nodeid, ua.DataValue(val))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # optional: setup logging
    # logger = logging.getLogger("asyncua.address_space")
    # logger.setLevel(logging.DEBUG)
    # logger = logging.getLogger("asyncua.internal_server")
    # logger.setLevel(logging.DEBUG)
    # logger = logging.getLogger("asyncua.binary_server_asyncio")
    # logger.setLevel(logging.DEBUG)
    # logger = logging.getLogger("asyncua.uaprocessor")
    # logger.setLevel(logging.DEBUG)

    asyncio.run(main())
