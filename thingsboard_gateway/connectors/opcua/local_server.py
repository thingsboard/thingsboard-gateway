import random
import uuid
from threading import Thread
import logging
from datetime import datetime
import time
from math import sin
import sys

from opcua.ua import NodeId, NodeIdType

sys.path.insert(0, "..")

try:
    from IPython import embed
except ImportError:
    import code


    def embed():
        myvars = globals()
        myvars.update(locals())
        shell = code.InteractiveConsole(myvars)
        shell.interact()

from opcua import ua, uamethod, Server


class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    """

    def datachange_notification(self, node, val, data):
        print("Python: New data change event", node, val)

    def event_notification(self, event):
        print("Python: New event", event)


# method to be exposed through server

def func(parent, variant):
    ret = False
    if variant.Value % 2 == 0:
        ret = True
    return [ua.Variant(ret, ua.VariantType.Boolean)]


# method to be exposed through server
# uses a decorator to automatically convert to and from variants

@uamethod
def multiply(_, x, y):
    print("multiply method call with parameters: ", x, y)
    return ua.Variant(x * y, ua.VariantType.Int64)


@uamethod
def set_relay_state(_, state):
    print("set_relay_state method call with parameters: ", state)
    try:
        state = bool(state)
        relay.set_value(state)
        return ua.Variant(True, ua.VariantType.Boolean)
    except Exception as e:
        print("Error setting relay state:", e)
        return ua.Variant(False, ua.VariantType.Boolean)


@uamethod
def get_relay_state(_):
    print("get_relay_state method call")
    try:
        state = relay.get_value()
        return ua.Variant(state, ua.VariantType.Boolean)
    except Exception as e:
        print("Error getting relay state:", e)
        return ua.Variant(False, ua.VariantType.Boolean)


class VarSinUpdater(Thread):
    def __init__(self, var):
        Thread.__init__(self)
        self._stopev = False
        self.var = var

    def stop(self):
        self._stopev = True

    def run(self):
        while not self._stopev:
            v = sin(time.time() / 10)
            self.var.set_value(v)
            time.sleep(1)


class VarIntUpdater(Thread):
    def __init__(self, var):
        Thread.__init__(self)
        self._stopev = False
        self.var = var

    def stop(self):
        self._stopev = True

    def run(self):
        while not self._stopev:
            v = random.randint(0, 100)
            self.var.set_value(v)
            time.sleep(1)


if __name__ == "__main__":
    # optional: setup logging
    logging.basicConfig(level=logging.DEBUG)

    # now setup our server
    server = Server()
    server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")
    server.set_server_name("FreeOpcUa Example Server")
    # set all possible endpoint policies for clients to connect through
    server.set_security_policy([
        ua.SecurityPolicyType.NoSecurity,
        ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
        ua.SecurityPolicyType.Basic256Sha256_Sign])

    # setup our own namespace
    uri = "http://examples.freeopcua.github.io"
    idx = server.register_namespace(uri)

    # DEVICES FOR BLACKBOX TESTS ---------------------------------------------------------------------------------------
    idx_for_tests = server.register_namespace("http://test.gateway.io")
    test_device = server.nodes.objects.add_object(idx_for_tests, "TempSensor")
    hum_var = test_device.add_variable("ns=3; s=Humidity", "Humidity", 60.5)
    hum_var.set_writable()
    press_var = test_device.add_variable(idx_for_tests, "Pressure", 1013.25)
    press_var.set_writable()
    test_device.add_variable(idx_for_tests, 'SomeText', "SomeText")
    e = test_device.add_variable(ua.ByteStringNodeId("MyNodeId".encode('utf-8'), namespace=3), "MyNodeId", 12.2)
    alarm_var = test_device.add_variable(ua.GuidNodeId(uuid.UUID("018dd02c-fd22-754a-b6d3-5fcae91cd38d"), 3), "Alarm",
                                         True)
    alarm_var.set_writable()
    alarm_var.add_variable(idx_for_tests, "AlarmCode", 1234)
    hum_var.add_reference(ua.ObjectIds.AnalogItemType, ua.ObjectIds.HasTypeDefinition)
    test_device.add_method(idx_for_tests, "multiply", multiply, [ua.VariantType.Int64, ua.VariantType.Int64],
                           [ua.VariantType.Int64])

    test_device_s = server.nodes.objects.add_object("ns=4; s=TempSensor_S", "TempSensor_S")
    hum_var_1 = test_device_s.add_variable("ns=4; s=Humidity_S", "Humidity", 243.5)
    hum_var_1.set_writable()
    press_var_1 = test_device_s.add_variable(idx_for_tests, "Pressure_S", 23455.25)
    press_var_1.set_writable()
    status_var_1 = test_device_s.add_variable("ns=4; b=Status_S", "Status", "ERROR")
    status_var_1.set_writable()
    alarm_var_1 = test_device_s.add_variable(ua.GuidNodeId(uuid.UUID("BAEAF004-1E43-4A06-9EF0-E52010D5CD12"), 4),
                                             "Alarm", False)
    alarm_var_1.set_writable()

    test_device_g = server.nodes.objects.add_object(
        ua.GuidNodeId(uuid.UUID("018dd02c-fd22-754a-b6d3-5fcae91cd39d"), 5), "TempSensor_G")
    hum_var_2 = test_device_g.add_variable("ns=5; s=Humidity_S", "Humidity", 243.5)
    hum_var_2.set_writable()
    press_var_2 = test_device_g.add_variable(idx_for_tests, "Pressure_S", 23455.25)
    press_var_2.set_writable()
    status_var_2 = test_device_g.add_variable("ns=5; b=Status_S", "Status", "ERROR")
    status_var_2.set_writable()
    alarm_var_2 = test_device_g.add_variable(ua.GuidNodeId(uuid.UUID("BAEAF004-1E43-4A06-9EF0-E52010D5CD12"), 5),
                                             "Alarm", False)
    alarm_var_1.set_writable()
    # ------------------------------------------------------------------------------------------------------------------

    # create a new node type we can instantiate in our address space
    dev = server.nodes.base_object_type.add_object_type(idx, "MyDevice")
    dev.add_variable(idx, "sensor1", 1.0).set_modelling_rule(True)
    dev.add_property(idx, "device_id", "0340").set_modelling_rule(True)
    ctrl = dev.add_object(idx, "controller")
    ctrl.set_modelling_rule(True)
    ctrl.add_property(idx, "state", "Idle").set_modelling_rule(True)

    # populating our address space

    # First a folder to organise our nodes
    myfolder = server.nodes.objects.add_folder(idx, "myEmptyFolder")
    # instanciate one instance of our device
    mydevice = server.nodes.objects.add_object(idx, "Device0001", dev)
    mydevice_var = mydevice.get_child(
        ["{}:controller".format(idx), "{}:state".format(idx)])  # get proxy to our device state variable
    # create directly some objects and variables
    myobj = server.nodes.objects.add_object(idx, "MyObject")
    relay = myobj.add_variable(idx, "Relay", False, ua.VariantType.Boolean)
    relay.set_writable()  # Set Relay to be writable by clients
    set_relay_node = myobj.add_method(idx, "set_relay", set_relay_state, [ua.VariantType.Boolean],
                                      [ua.VariantType.Boolean])
    get_relay_node = myobj.add_method(idx, "get_relay", get_relay_state, [], [ua.VariantType.Boolean])
    myvar = myobj.add_variable(idx, "Frequency", 6, ua.VariantType.Int16)
    mysin = myobj.add_variable(idx, "Power", 0, ua.VariantType.Float)
    temperature = myobj.add_variable(idx, "Temperature", 0, ua.VariantType.Int16)
    humidity = myobj.add_variable(idx, "Humidity", 0, ua.VariantType.Int16)
    myvar.set_writable()  # Set MyVariable to be writable by clients
    mystringvar = myobj.add_variable(idx, "MyStringVariable", "Really nice string")
    mystringvar.set_writable()  # Set MyVariable to be writable by clients
    myguidvar = myobj.add_variable(NodeId(uuid.UUID('1be5ba38-d004-46bd-aa3a-b5b87940c698'), idx, NodeIdType.Guid),
                                   'MyStringVariableWithGUID', 'NodeId type is guid')
    mydtvar = myobj.add_variable(idx, "MyDateTimeVar", datetime.utcnow())
    mydtvar.set_writable()  # Set MyVariable to be writable by clients
    myarrayvar = myobj.add_variable(idx, "myarrayvar", [6.7, 7.9])
    myprop = myobj.add_property(idx, "myproperty", "I am a property")
    mymethod = myobj.add_method(idx, "mymethod", func, [ua.VariantType.Int64], [ua.VariantType.Boolean])
    multiply_node = myobj.add_method(idx, "multiply", multiply, [ua.VariantType.Int64, ua.VariantType.Int64],
                                     [ua.VariantType.Int64])

    myevgen = server.get_event_generator()
    myevgen.event.Severity = 3000

    # starting!
    server.start()
    print("Available loggers are: ", logging.Logger.manager.loggerDict.keys())
    vup = VarSinUpdater(mysin)
    vup.start()

    vup1 = VarIntUpdater(myvar)
    vup1.start()

    vup2 = VarIntUpdater(temperature)
    vup2.start()

    vup3 = VarIntUpdater(humidity)
    vup3.start()