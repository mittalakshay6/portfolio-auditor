def get_matching_scheme_from_isin(isin: str, schemes: list):
    for scheme in schemes:
        if scheme['isin'] == isin:
            return scheme
    return None
