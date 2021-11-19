import datetime
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes
except ImportError:
    print("Requests library not found - installing...")
    TBUtility.install_package("cryptography")
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes


class SSLGenerator:
    def __init__(self, hostname):
        self.hostname: str = hostname

    def generate_certificate(self):
        key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend(),
        )

        with open("domain_srv.key", "wb") as f:
            f.write(key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            ))

        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, u"US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"CA"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, u"locality"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"A place"),
            x509.NameAttribute(NameOID.COMMON_NAME, self.hostname),
        ])

        cert = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer
        ).public_key(
            key.public_key()
        ).serial_number(
            x509.random_serial_number()
        ).not_valid_before(
            datetime.datetime.utcnow()
        ).not_valid_after(
            datetime.datetime.utcnow() + datetime.timedelta(days=365)
        ).add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName(u"localhost"),
                x509.DNSName(self.hostname),
                x509.DNSName(u"127.0.0.1")]),
            critical=False,
        ).sign(key, hashes.SHA256(), default_backend())

        with open("domain_srv.crt", "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
