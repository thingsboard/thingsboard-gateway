from bacpypes3.appservice import ApplicationServiceAccessPoint as AppServiceAccessPoint, ServiceAccessPoint
from bacpypes3.apdu import (
    APCISequence,
    APDU,
    AbortPDU,
    ComplexAckPDU,
    ConfirmedRequestPDU,
    ErrorPDU,
    RejectPDU,
    SimpleAckPDU,
    UnconfirmedRequestPDU,
)


class ApplicationServiceAccessPoint(AppServiceAccessPoint):
    async def sap_response(self, apdu: APDU) -> None:
        if isinstance(apdu, SimpleAckPDU):
            xpdu = apdu

        elif isinstance(
            apdu, (UnconfirmedRequestPDU, ConfirmedRequestPDU, ComplexAckPDU, ErrorPDU)
        ):
            try:
                xpdu = APCISequence.decode(apdu)
            except Exception:
                if isinstance(apdu, UnconfirmedRequestPDU):
                    return

                if isinstance(apdu, ConfirmedRequestPDU):
                    return

                xpdu = apdu
        elif isinstance(apdu, (RejectPDU, AbortPDU)):
            xpdu = apdu
        else:
            raise RuntimeError(f"invalid APDU (10): {type(apdu)}")

        await ServiceAccessPoint.sap_response(self, xpdu)
