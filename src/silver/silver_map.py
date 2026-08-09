payment_map = {
    # "0": "Flex Fare trip",

    # Cash
    'cash': '2',
    "cas": "2",
    "csh": "2",
    # "2": "Cash",

    # Credit card
    "credit": "1",
    "crd": "1",
    "cre": "1",
    "1": "1",

    # No charge
    "no charge": "3",
    "no": "3",
    "noc": "3",
    # "3": "No charge",

    # Dispute
    "dispute": "4",
    "dis": "4",
    # "4": "Dispute",

    # Unknown
    "na": "5",
    # "5": "Unknown",

    # "6": "Voided trip",
}

store_fwd_flag_map = {
    # yes
    # '1': 'Yes',
    '1.0': '1',
    'y': '1',
    'yes': '1',

    # no
    # '0': 'No',
    '0.0': '0',
    'n': '0',
    'no': '0',

    # null
    'nan': None,
    '': None,
}

vendor_id_map = {
    'dds': '8',

    # "1": "Creative Mobile Technologies, LLC",
    'cmt': '1',

    # "2": "Curb Mobility, LLC",
    'vts': '2',

    # "6": "Myle Technologies Inc",

    # "7": "Helix",
}
