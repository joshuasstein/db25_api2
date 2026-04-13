# -*- coding: utf-8 -*-
"""
Created on Mon Dec 15 14:09:21 2025

@author: bathomp
"""

import requests
import pandas as pd
from io import StringIO


def get_token(username, password):
    url = "http://10.252.9.176:8080/login"

    response = requests.post(
        url,
        json={
            "username": username,
            "password": password,
        },
        proxies={
            "http": None,
            "https": None,
        },
        timeout=10,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Login failed ({response.status_code}): {response.text}"
        )

    return response.json()["access_token"]

def get_data(endpoint, access_token):
    url = f"http://10.252.9.176:8080/{endpoint.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    response = requests.get(
        url,
        headers=headers,
        proxies={
            "http": None,
            "https": None,
        },
        timeout=10,
    )

    if response.status_code == 200:
        # Try JSON first
        try:
            return response.json()
        except ValueError:
            # Fallback: CSV
            csv_data = StringIO(response.text)
            return pd.read_csv(csv_data)
    else:
        raise RuntimeError(
            f"Request failed ({response.status_code}): {response.text}"
        )

def get_data_with_params(endpoint, token, params=None):
    url = f"http://10.252.9.176:8080/{endpoint.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Disable proxy explicitly
    resp = requests.get(
        url,
        headers=headers,
        params=params,
        proxies={"http": None, "https": None},
    )

    resp.raise_for_status()

    try:
        return resp.json()
    except ValueError:
        from io import StringIO
        import pandas as pd
        return pd.read_csv(StringIO(resp.text))
    
#token = get_token('brent', 'thompson')

#data = get_data_with_params("v", token)

#params = {"start_date":'2025-01-01T12:00:00-07:00',
 #         "end_date":'2025-01-01T13:00:00-07:00'}     #default will show all subsystems and labels
#out = get_data_with_params('v1/systems/SLTE_PSEL_LG1/measurements', token, params)
