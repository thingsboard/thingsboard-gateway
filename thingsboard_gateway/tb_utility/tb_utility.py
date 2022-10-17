#     Copyright 2022. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
import datetime
from logging import getLogger
from re import search, findall

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
from jsonpath_rw import parse
from simplejson import JSONDecodeError, dumps, loads

log = getLogger("service")


class TBUtility:

    @staticmethod
    def decode(message):
        try:
            if isinstance(message.payload, bytes):
                content = loads(message.payload.decode("utf-8", "ignore"))
            else:
                content = loads(message.payload)
        except JSONDecodeError:
            try:
                content = message.payload.decode("utf-8", "ignore")
            except JSONDecodeError:
                content = message.payload
        return content

    @staticmethod
    def validate_converted_data(data):
        error = None
        if error is None and not data.get("deviceName"):
            error = 'deviceName is empty in data: '

        if error is None:
            got_attributes = False
            got_telemetry = False

            if data.get("attributes") is not None and len(data.get("attributes")) > 0:
                got_attributes = True

            if data.get("telemetry") is not None:
                for entry in data.get("telemetry"):
                    if (entry.get("ts") is not None and len(entry.get("values")) > 0) or entry.get("ts") is None:
                        got_telemetry = True
                        break

            if got_attributes is False and got_telemetry is False:
                error = 'No telemetry and attributes in data: '

        if error is not None:
            json_data = dumps(data)
            if isinstance(json_data, bytes):
                log.error(error + json_data.decode("UTF-8"))
            else:
                log.error(error + json_data)
            return False
        return True

    @staticmethod
    def topic_to_regex(topic):
        return topic.replace("+", "[^/]+").replace("#", ".+").replace('$', '\\$')

    @staticmethod
    def regex_to_topic(regex):
        return regex.replace("[^/]+", "+").replace(".+", "#").replace('\\$', '$')

    @staticmethod
    def get_value(expression, body=None, value_type="string", get_tag=False, expression_instead_none=False):
        if isinstance(body, str):
            body = loads(body)
        if not expression:
            return ''
        positions = search(r'\${(?:(.*))}', expression)
        if positions is not None:
            p1 = positions.regs[-1][0]
            p2 = positions.regs[-1][1]
        else:
            p1 = 0
            p2 = len(expression)
        target_str = str(expression[p1:p2])
        if get_tag:
            return target_str
        full_value = None
        try:
            if isinstance(body, dict) and target_str.split()[0] in body:
                if value_type.lower() == "string":
                    full_value = str(expression[0: max(p1 - 2, 0)]) + str(body[target_str.split()[0]]) + str(expression[
                                                                                                             p2 + 1:len(
                                                                                                                 expression)])
                else:
                    full_value = body.get(target_str.split()[0])
            elif isinstance(body, (dict, list)):
                try:
                    jsonpath_expression = parse(target_str)
                    jsonpath_match = jsonpath_expression.find(body)
                    if jsonpath_match:
                        full_value = jsonpath_match[0].value
                except Exception as e:
                    log.debug(e)
            elif isinstance(body, (str, bytes)):
                search_result = search(expression, body)
                if search_result.groups():
                    full_value = search_result.group(0)
            if expression_instead_none and full_value is None:
                full_value = expression
        except Exception as e:
            log.exception(e)
        return full_value

    @staticmethod
    def get_values(expression, body=None, value_type="string", get_tag=False, expression_instead_none=False):
        expression_arr = findall(r'\$\{[${A-Za-z0-9.^\]\[*_:]*\}', expression)

        values = [TBUtility.get_value(exp, body, value_type=value_type, get_tag=get_tag,
                                      expression_instead_none=expression_instead_none) for exp in expression_arr]

        if '${' not in expression:
            values.append(expression)

        return values

    @staticmethod
    def install_package(package, version="upgrade"):
        from sys import executable
        from subprocess import check_call, CalledProcessError
        result = False
        if version.lower() == "upgrade":
            try:
                result = check_call([executable, "-m", "pip", "install", package, "--upgrade", "--user"])
            except CalledProcessError:
                result = check_call([executable, "-m", "pip", "install", package, "--upgrade"])
        else:
            from pkg_resources import get_distribution
            current_package_version = None
            try:
                current_package_version = get_distribution(package)
            except Exception:
                pass
            if current_package_version is None or current_package_version != version:
                installation_sign = "==" if ">=" not in version else ""
                try:
                    result = check_call(
                        [executable, "-m", "pip", "install", package + installation_sign + version, "--user"])
                except CalledProcessError:
                    result = check_call([executable, "-m", "pip", "install", package + installation_sign + version])
        return result

    @staticmethod
    def replace_params_tags(text, data):
        if '${' in text:
            for item in text.split('/'):
                if '${' in item:
                    tag = '${' + TBUtility.get_value(item, data['data'], 'params', get_tag=True) + '}'
                    value = TBUtility.get_value(item, data['data'], 'params', expression_instead_none=True)
                    text = text.replace(tag, str(value))
        return text

    @staticmethod
    def get_dict_key_by_value(dictionary: dict, value):
        return list(dictionary.values())[list(dictionary.values()).index(value)]

    @staticmethod
    def generate_certificate(old_certificate_path, old_key_path, old_certificate):
        key = ec.generate_private_key(ec.SECP256R1())
        public_key = key.public_key()
        builder = x509.CertificateBuilder()
        builder = builder.subject_name(old_certificate.subject)
        builder = builder.issuer_name(old_certificate.issuer)
        builder = builder.not_valid_before(datetime.datetime.today() - datetime.timedelta(days=1))
        builder = builder.not_valid_after(datetime.datetime.today() + (datetime.timedelta(1, 0, 0) * 365))
        builder = builder.serial_number(x509.random_serial_number())
        builder = builder.public_key(public_key)
        certificate = builder.sign(private_key=key, algorithm=hashes.SHA256())

        cert = certificate.public_bytes(serialization.Encoding.PEM)
        with open(old_certificate_path, 'wb+') as f:
            f.write(cert)

        key = key.private_bytes(encoding=serialization.Encoding.PEM,
                                format=serialization.PrivateFormat.TraditionalOpenSSL,
                                encryption_algorithm=serialization.NoEncryption())
        with open(old_key_path, 'wb+') as f:
            f.write(key)

        return cert

    @staticmethod
    def check_certificate(certificate, key=None, generate_new=True, days_left=3):
        cert_detail = x509.load_pem_x509_certificate(open(certificate, 'rb').read())

        if cert_detail.not_valid_after - datetime.datetime.now() <= datetime.timedelta(days=days_left):
            if generate_new:
                return TBUtility.generate_certificate(certificate, key, cert_detail)
            else:
                return True
