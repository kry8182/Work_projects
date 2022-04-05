-- Итог расходов
update fin set itogo = (select (lise+
    x+
    topl+
    meg+
    meg2+
    bil+
    internet+
    gkh+
    prod+
    eg+
    zapas+
    apteka+
    podarki+
    ugoshen+
    hoztovar+
    neobhodimoe+
    rashodniki+
    obrazovan+
    kats+
    detsk_kapital+
    detsk_tekush+
    detsk_apt+
    detsk_med+
    odegda_obuv+
    kanctov+
    rash_printera+
    profilakt+
    medicine+
    modernization+
    proch+
    T3+
    matiz+
    shtrafs+
    pogertv+
    remonts+
    ubytki+
    selhoz+
    PSTGU+
    transport+
    sport+
    hobbi+
    dosug+
    kvart+
    nalogi+
    knigi+
    revival+
    stroyka+
    instrument+
    dostavka+
    vdolg+
    otpusk+
    largus+
    zp+
    derev_infrastructur+
    obsl_doma+
    obsheavto+
    kolhoz+
    avtodor+
    vsevologsk_infrastructur) from fin
where m_date = timestamp '{date}')
where m_date = timestamp '{date}'

-- Итог доходов
update fin set itogo_in = (select (posobia+
    liza_in+
    lesha_in+
    x_cpsh+
    auto_comp+
    rent+
    podarki_in+
    bonus+
    procent+
    vozvrat) from fin
where m_date = timestamp '{date}')
where m_date = timestamp '{date}'

