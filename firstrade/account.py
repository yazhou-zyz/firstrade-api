import os
import pickle
import re

import requests
from bs4 import BeautifulSoup
import pyotp

from firstrade import urls


class FTSession:
    """Class creating a session for Firstrade."""

    def __init__(self, username, password, pin,totp_secret, profile_path=None):
        """
        Initializes a new instance of the FTSession class.

        Args:
            username (str): Firstrade login username.
            password (str): Firstrade login password.
            pin (str): Firstrade login pin.
            persistent_session (bool, optional): Whether the user wants to save the session cookies.
            profile_path (str, optional): The path where the user wants to save the cookie pkl file.
        """
        self.username = username
        self.password = password
        self.pin = pin
        self.profile_path = profile_path
        self.session = requests.Session()
        self.totp_secret = totp_secret  # Google Authenticator密钥
        self.login()

    def login(self):
        headers = urls.session_headers()
        cookies = self.load_cookies()
        cookies = requests.utils.cookiejar_from_dict(cookies)
        self.session.cookies.update(cookies)
        response = self.session.get(
            url=urls.get_xml(), headers=urls.session_headers(), cookies=cookies
        )
        if response.status_code != 200:
            raise Exception(
                "Login failed. Check your credentials or internet connection."
            )
        if "/cgi-bin/sessionfailed?reason=6" in response.text:
            self.session.get(url=urls.login(), headers=headers)
            data = {
                "redirect": "",
                "ft_locale": "en-us",
                "login.x": "Log In",
                "username": r"" + self.username,
                "password": r"" + self.password,
                "destination_page": "home",
            }

            response = self.session.post(
                url=urls.login(),
                headers=headers,
                cookies=self.session.cookies,
                data=data,
            )

            # 检查是否需要输入2FA代码
            if "2FA" in response.text:  # 这里需要根据实际的响应内容来判断
                totp = pyotp.TOTP(self.totp_secret)
                two_factor_code = totp.now()

                # 发送2FA验证请求
                data = {
                    "two_factor_code": two_factor_code,
                    # 可能需要其他字段，根据Firstrade的实际要求添加
                }
                response = self.session.post(
                    url=urls.two_factor_auth(),  # 需要定义这个URL
                    headers=headers,
                    cookies=self.session.cookies,
                    data=data,
                )

                # 检查2FA验证是否成功
                if "2FA Failed" in response.text:  # 根据实际响应修改
                    raise Exception("Two-factor authentication failed.")

            data = {
                "destination_page": "home",
                "pin": self.pin,
                "pin.x": "++OK++",
                "sring": "0",
                "pin": self.pin,
            }

            self.session.post(
                url=urls.pin(), headers=headers, cookies=self.session.cookies, data=data
            )
            self.save_cookies()
        if (
                "/cgi-bin/sessionfailed?reason=6"
                in self.session.get(
            url=urls.get_xml(), headers=urls.session_headers(), cookies=cookies
        ).text
        ):
            raise Exception("Login failed. Check your credentials.")

    def load_cookies(self):
        """
        Checks if session cookies were saved.

        Returns:
            Dict: Dictionary of cookies. Nom Nom
        """
        cookies = {}
        directory = os.path.abspath(self.profile_path) if self.profile_path is not None else "."
        
        if not os.path.exists(directory):
            os.makedirs(directory)

        for filename in os.listdir(directory):
            if filename.endswith(f"{self.username}.pkl"):
                filepath = os.path.join(directory, filename)
                with open(filepath, "rb") as f:
                    cookies = pickle.load(f)
        return cookies

    def save_cookies(self):
        """Saves session cookies to a file."""
        if self.profile_path is not None:
            directory = os.path.abspath(self.profile_path)
            if not os.path.exists(directory):
                os.makedirs(directory)
            path = os.path.join(self.profile_path, f"ft_cookies{self.username}.pkl")
        else:
            path = f"ft_cookies{self.username}.pkl"
        with open(path, "wb") as f:
            pickle.dump(self.session.cookies.get_dict(), f)
    
    def delete_cookies(self):    
        """Deletes the session cookies."""
        if self.profile_path is not None:
            path = os.path.join(self.profile_path, f"ft_cookies{self.username}.pkl")
        else:
            path = f"ft_cookies{self.username}.pkl"
        os.remove(path)

    def __getattr__(self, name):
        """
        Forwards unknown attribute access to session object.

        Args:
            name (str): The name of the attribute to be accessed.

        Returns:
            The value of the requested attribute from the session object.
        """
        return getattr(self.session, name)


class FTAccountData:
    """Dataclass for storing account information."""

    def __init__(self, session):
        """
        Initializes a new instance of the FTAccountData class.

        Args:
            session (requests.Session):
            The session object used for making HTTP requests.
        """
        self.session = session
        self.all_accounts = []
        self.account_numbers = []
        self.account_statuses = []
        self.account_balances = []
        self.securities_held = {}
        all_account_info = []
        html_string = self.session.get(
            url=urls.account_list(),
            headers=urls.session_headers(),
            cookies=self.session.cookies,
        ).text
        regex_accounts = re.findall(r"([0-9]+)-", html_string)

        for match in regex_accounts:
            self.account_numbers.append(match)

        for account in self.account_numbers:
            # reset cookies to base login cookies to run scripts
            self.session.cookies.clear()
            self.session.cookies.update(self.session.load_cookies())
            # set account to get data for
            data = {"accountId": account}
            self.session.post(
                url=urls.account_status(),
                headers=urls.session_headers(),
                cookies=self.session.cookies,
                data=data,
            )
            # request to get account status data
            data = {"req": "get_status"}
            account_status = self.session.post(
                url=urls.status(),
                headers=urls.session_headers(),
                cookies=self.session.cookies,
                data=data,
            ).json()
            self.account_statuses.append(account_status["data"])
            data = {"page": "bal", "account_id": account}
            account_soup = BeautifulSoup(
                self.session.post(
                    url=urls.get_xml(),
                    headers=urls.session_headers(),
                    cookies=self.session.cookies,
                    data=data,
                ).text,
                "xml",
            )
            balance = account_soup.find("total_account_value").text
            self.account_balances.append(balance)
            all_account_info.append(
                {
                    account: {
                        "Balance": balance,
                        "Status": {
                            "primary": account_status["data"]["primary"],
                            "domestic": account_status["data"]["domestic"],
                            "joint": account_status["data"]["joint"],
                            "ira": account_status["data"]["ira"],
                            "hasMargin": account_status["data"]["hasMargin"],
                            "opLevel": account_status["data"]["opLevel"],
                            "p_country": account_status["data"]["p_country"],
                            "mrgnStatus": account_status["data"]["mrgnStatus"],
                            "opStatus": account_status["data"]["opStatus"],
                            "margin_id": account_status["data"]["margin_id"],
                        },
                    }
                }
            )

        self.all_accounts = all_account_info

    def get_positions(self, account):
        """Gets currently held positions for a given account.

        Args:
            account (str): Account number of the account you want to get positions for.

        Returns:
            self.securities_held {dict}:
            Dict of held positions with the pos. ticker as the key.
        """
        data = {
            "page": "pos",
            "accountId": str(account),
        }
        position_soup = BeautifulSoup(
            self.session.post(
                url=urls.get_xml(),
                headers=urls.session_headers(),
                data=data,
                cookies=self.session.cookies,
            ).text,
            "xml",
        )

        tickers = position_soup.find_all("symbol")
        quantity = position_soup.find_all("quantity")
        price = position_soup.find_all("price")
        change = position_soup.find_all("change")
        change_percent = position_soup.find_all("changepercent")
        vol = position_soup.find_all("vol")
        for i, ticker in enumerate(tickers):
            ticker = ticker.text
            self.securities_held[ticker] = {
                "quantity": quantity[i].text,
                "price": price[i].text,
                "change": change[i].text,
                "change_percent": change_percent[i].text,
                "vol": vol[i].text,
            }
        return self.securities_held
