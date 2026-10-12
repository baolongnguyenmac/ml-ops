payment_map = {
    "0": "Flex Fare trip",

    # Cash
    'cash': 'Cash',
    "cas": "Cash",
    "csh": "Cash",
    "2": "Cash",

    # Credit card
    "credit": "Credit card",
    "crd": "Credit card",
    "cre": "Credit card",
    "1": "Credit card",

    # No charge
    "no charge": "No charge",
    "no": "No charge",
    "noc": "No charge",
    "3": "No charge",

    # Dispute
    "dispute": "Dispute",
    "dis": "Dispute",
    "4": "Dispute",

    # Unknown
    "na": "Unknown",
    "5": "Unknown",

    "6": "Voided trip",
}
inverse_payment_map = {
    "Flex Fare trip": "0",
    "Credit card": "1",
    "Cash": "2",
    "No charge": "3",
    "Dispute": "4",
    "Unknown": "5",
    "Voided trip": "6",
}

store_fwd_flag_map = {
    # yes
    '1': 'Yes',
    '1.0': 'Yes',
    'y': 'Yes',

    # no
    '0': 'No',
    '0.0': 'No',
    'n': 'No',

    # null
    'nan': None,
    '': None,
}
inverse_store_fwd_flag_map = {
    'Yes': '1',
    'No': '0',
}

vendor_id_map = {
    'dds': 'DDS',

    "1": "Creative Mobile Technologies, LLC",
    'cmt': 'Creative Mobile Technologies, LLC',

    'vts': 'Curb Mobility, LLC',
    "2": "Curb Mobility, LLC",

    "6": "Myle Technologies Inc",

    "7": "Helix",
}
inverse_vendor_id_map = {
    "Creative Mobile Technologies, LLC": "1",
    "Curb Mobility, LLC": "2",
    "Myle Technologies Inc": "6",
    "Helix": "7",
    'DDS': 'dds',
}

rate_code_id_map = {
    "1.0": "Standard rate",
    "2.0": "JFK",
    "3.0": "Newark",
    "4.0": "Nassau or Westchester",
    "5.0": "Negotiated fare",
    "6.0": "Group ride",
    "99.0": "Null/unknown",
}
inverse_rate_code_id_map = {
    "Standard rate": "1",
    "JFK": "2",
    "Newark": "3",
    "Nassau or Westchester": "4",
    "Negotiated fare": "5",
    "Group ride": "6",
    "Null/unknown": "99",
}
