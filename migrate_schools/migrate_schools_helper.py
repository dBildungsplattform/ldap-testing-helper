def classify_dnr(dnr):
    if dnr.startswith('070'):
        return 'OEFFENTLICH'
    elif dnr.startswith('07998') or dnr.startswith('079'):
        return 'ERSATZ'
    else:
        return 'SONSTIGE'