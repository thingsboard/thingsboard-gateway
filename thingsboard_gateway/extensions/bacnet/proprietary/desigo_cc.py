"""
#   Siemens Desigo CC
#   Register proprietary Objects and Properties
"""

from bacpypes3.constructeddata import ArrayOf
from bacpypes3.vendor import VendorInfo
from bacpypes3.basetypes import PropertyIdentifier
from bacpypes3.object import AnalogInputObject as _AnalogInputObject
from bacpypes3.object import PulseConverterObject as _PulseConverterObject
from bacpypes3.object import AnalogValueObject as _AnalogValueObject
from bacpypes3.object import DeviceObject as _DeviceObject
from bacpypes3.object import NetworkPortObject as _NetworkPortObject
from bacpypes3.primitivedata import (
    ObjectType,
    CharacterString,
)


# this vendor identifier reference is used when registering custom classes
_vendor_id = 7
_vendor_name = "Siemens Building Technologies"


class ProprietaryObjectType(ObjectType):
    """
    This is a list of the object type enumerations for proprietary object types,
    see Clause 23.4.1.
    """

    pass


class ProprietaryPropertyIdentifier(PropertyIdentifier):
    """
    This is a list of the property identifiers that are used in custom object
    types or are used in custom properties of standard types.
    """

    descriptionList = 3121


# create a VendorInfo object for this custom application before registering
# specialize object classes
_desigo_cc = VendorInfo(
    _vendor_id, ProprietaryObjectType, ProprietaryPropertyIdentifier
)


class DesigoCCDeviceObject(_DeviceObject):
    """
    When running as an instance of this custom device, the DeviceObject is
    an extension of the one defined in bacpypes3.device
    """

    descriptionList: CharacterString


class NetworkPortObject(_NetworkPortObject):
    """
    When running as an instance of this custom device, the NetworkPortObject is
    an extension of the one defined in bacpypes3.networkport (in this
    case doesn't add any proprietary properties).
    """

    pass


class DesigoCCAnalogInputObject(_AnalogInputObject):
    descriptionList: ArrayOf(CharacterString)


class DesigoCCPulseConverterObject(_PulseConverterObject):
    descriptionList: ArrayOf(CharacterString)


class DesigoCCAnalogValueObject(_AnalogValueObject):
    descriptionList: ArrayOf(CharacterString)
