const moment = require("moment-timezone");
const {
    map,
    pipe,
    flatten,
    values,
    head,
    identity,
    of,
    defaultTo,
    toLower,
    sum,
    curry,
    uniq,
    reject,
    paths,
    pick,
    not,
    mergeAll,
    hasPath,
    anyPass,
} = require("ramda");
const { size, isUndefined, uniqBy: lodashUniqBy, compact, isEmpty, uniqBy, toNumber, groupBy: lodashGroupBy, flattenDeep } = require("lodash");
const { from, zip, of: rxof, concatMap, map: rxmap, filter: rxfilter, reduce: rxreduce, defaultIfEmpty, catchError, iif, tap } = require("rxjs");
const { db } = require("./database");
const { query, where, getDoc, getDocs, collection, collectionGroup, doc, limit } = require("firebase/firestore");
const { get, all, mod, into, matching } = require("shades");
const { Facebook: RoasFacebook } = require("roasfacebook");
const { logroupby, lokeyby, pipeLog, louniqby, lofilter, loorderby } = require("helpers");

const ipEvents = curry((version, ip, roas_user_id) => {
    let q = query(collection(db, "events"), where(version, "==", ip), where("roas_user_id", "==", roas_user_id));
    return from(getDocs(q)).pipe(rxmap(ClickFunnels.utilities.queryDocs));
});

const Facebook = {
    ads: {
        details: {
            get: ({ ad_ids = [], user_id, fb_ad_account_id, date } = {}) => {
                let func_name = `Facebook:ads:details`;
                console.log(func_name);

                if (!ad_ids) return throwError(`error:${func_name}:no ad_ids`);
                if (!user_id) return throwError(`error:${func_name}:no user_id`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                return from(ad_ids).pipe(
                    concatMap((ad_meta_data) => {
                        let { ad_id } = ad_meta_data;
                        let ad_args = { ad_id, date, user_id, fb_ad_account_id };

                        return Facebook.ad.db.get(ad_args).pipe(
                            concatMap((ad) => iif(() => !isEmpty(ad), rxof(ad), Facebook.ad.api.get(ad_args))),
                            rxfilter(pipe(isEmpty, not))
                        );
                    }),
                    defaultIfEmpty([])
                );
            },
        },
    },

    ad: {
        details: (ad) => {
            let func_name = `Facebook:ad:details`;
            console.log(func_name);

            if (ad.details) {
                return {
                    account_id: ad.account_id,
                    asset_id: ad.details.ad_id,
                    asset_name: ad.details.ad_name,
                    campaign_id: ad.details.campaign_id,
                    campaign_name: ad.details.campaign_name,
                    adset_id: ad.details.adset_id,
                    adset_name: ad.details.adset_name,
                    ad_id: ad.details.ad_id,
                    ad_name: ad.details.ad_name,
                    name: ad.details.ad_name,
                };
            } else {
                return {
                    account_id: ad.account_id,
                    asset_id: ad.id,
                    asset_name: ad.name,
                    campaign_id: ad.campaign_id,
                    campaign_name: ad.campaign_name,
                    adset_id: ad.adset_id,
                    adset_name: ad.adset_name,
                    ad_id: ad.id,
                    ad_name: ad.name,
                    name: ad.name,
                };
            }
        },

        db: {
            get: ({ ad_id, date, user_id, fb_ad_account_id } = {}) => {
                let func_name = `Facebook:ad:db:get`;
                console.log(func_name);

                if (!ad_id) return throwError(`error:${func_name}:no ad_id`);
                if (!date) return throwError(`error:${func_name}:no date`);
                if (!user_id) return throwError(`error:${func_name}:no user_id`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                return from(RoasFacebook({ user_id }).ad.get_from_db({ ad_id })).pipe(
                    concatMap(identity),
                    rxfilter((ad) => !isEmpty(ad)),
                    rxmap(Facebook.ad.details),
                    defaultIfEmpty({}),
                    catchError((error) => {
                        console.log("Facebook:ad:db:get:error");
                        console.log(error);
                        return rxof({ ad_id, error: true });
                    })
                );
            },
        },

        api: {
            get: ({ ad_id, date, user_id, fb_ad_account_id } = {}) => {
                let func_name = `Facebook:ad:api:get`;
                console.log(func_name);

                if (!ad_id) return throwError(`error:${func_name}:no ad_id`);
                if (!date) return throwError(`error:${func_name}:no date`);
                if (!user_id) return throwError(`error:${func_name}:no user_id`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                return from(
                    getDocs(query(collectionGroup(db, "integrations"), where("user_id", "==", user_id), where("account_name", "==", "facebook")))
                ).pipe(
                    rxmap(ClickFunnels.utilities.queryDocs),
                    rxmap(head),
                    concatMap((fb_account_credentials) => {
                        if (!fb_account_credentials) return rxof([]);
                        let { access_token = "" } = fb_account_credentials;
                        if (size(access_token) == 0) return rxof([]);

                        let facebook = RoasFacebook({ user_id });

                        return from(facebook.ad.get({ ad_id, date, fb_ad_account_id, access_token })).pipe(
                            rxmap(pipe(values, head)),
                            rxfilter((ad) => !isUndefined(ad.id)),
                            concatMap((ad) => {
                                let adset = Facebook.ad.adset.api.get({ adset_id: ad.adset_id, user_id, date, fb_ad_account_id });
                                let campaign = Facebook.ad.campaign.api.get({ campaign_id: ad.campaign_id, user_id, date, fb_ad_account_id });

                                return zip([adset, campaign]).pipe(
                                    rxmap(([{ name: adset_name }, { name: campaign_name }]) => ({ ...ad, adset_name, campaign_name })),
                                    rxmap(Facebook.ad.details)
                                );
                            })
                        );
                    }),
                    defaultIfEmpty({}),
                    catchError((error) => {
                        console.log("Facebook:ad:api:get:error");
                        console.log(error);
                        return rxof({ ad_id, error: true });
                    })
                );
            },
        },

        adset: {
            api: {
                get: ({ adset_id, user_id, date, fb_ad_account_id } = {}) => {
                    let func_name = `Facebook:ad:adset:api:get`;
                    console.log(func_name);

                    if (!adset_id) return throwError(`error:${func_name}:no adset_id`);
                    if (!date) return throwError(`error:${func_name}:no date`);
                    if (!user_id) return throwError(`error:${func_name}:no user_id`);
                    if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                    return from(
                        getDocs(query(collectionGroup(db, "integrations"), where("user_id", "==", user_id), where("account_name", "==", "facebook")))
                    ).pipe(
                        rxmap(ClickFunnels.utilities.queryDocs),
                        rxmap(head),
                        concatMap((fb_account_credentials) => {
                            if (!fb_account_credentials) return rxof([]);
                            let { access_token = "" } = fb_account_credentials;
                            if (size(access_token) == 0) return rxof([]);

                            let facebook = RoasFacebook({ user_id });

                            return from(facebook.adset.get({ adset_id, date, fb_ad_account_id, access_token })).pipe(rxmap(pipe(values, head)));
                        }),
                        defaultIfEmpty({})
                    );
                },
            },
        },

        campaign: {
            api: {
                get: ({ campaign_id, user_id, date, fb_ad_account_id } = {}) => {
                    let func_name = `Facebook:ad:campaign:api:get`;
                    console.log(func_name);

                    if (!campaign_id) return throwError(`error:${func_name}:no campaign_id`);
                    if (!date) return throwError(`error:${func_name}:no date`);
                    if (!user_id) return throwError(`error:${func_name}:no user_id`);
                    if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                    return from(
                        getDocs(query(collectionGroup(db, "integrations"), where("user_id", "==", user_id), where("account_name", "==", "facebook")))
                    ).pipe(
                        rxmap(ClickFunnels.utilities.queryDocs),
                        rxmap(head),
                        concatMap((fb_account_credentials) => {
                            if (!fb_account_credentials) return rxof([]);
                            let { access_token = "" } = fb_account_credentials;
                            if (size(access_token) == 0) return rxof([]);

                            let facebook = RoasFacebook({ user_id });
                            return from(facebook.campaign.get({ campaign_id, date, fb_ad_account_id, access_token })).pipe(rxmap(pipe(values, head)));
                        }),
                        defaultIfEmpty({})
                    );
                },
            },
        },
    },
};

const Events = {
    ip: (version, ip) => {
        let q = query(collection(db, "events"), where(version, "==", ip));
        return from(getDocs(q)).pipe(rxmap(ClickFunnels.utilities.queryDocs));
    },
};

const Event = {
    ad: {
        id: ({ fb_ad_id, h_ad_id, ad_id, fb_id } = {}) => {
            let func_name = `Event:ad:id`;
            console.log(func_name);

            if (fb_id) {
                return fb_id;
            }

            if (ad_id) {
                return ad_id;
            }

            if (fb_ad_id && h_ad_id) {
                if (fb_ad_id == h_ad_id) {
                    return fb_ad_id;
                }

                if (fb_ad_id !== h_ad_id) {
                    return h_ad_id;
                }
            }

            if (fb_ad_id && !h_ad_id) {
                return fb_ad_id;
            }

            if (h_ad_id && !fb_ad_id) {
                return h_ad_id;
            }
        },
    },

    ip: {
        v4: (event) => {
            let func_name = `Event:ip:v4`;
            console.log(func_name);
            return pipe(get("ipv4"))(event);
        },

        v6: (event) => {
            let func_name = `Event:ip:v6`;
            console.log(func_name);
            return pipe(get("ipv6"))(event);
        },
    },

    user_agent: (event) => {
        let func_name = `Event:user_agent`;
        console.log(func_name);
        return pipe(get("userAgent"))(event);
    },

    get_utc_timestamp: (value) => {
        console.log("get_utc_timestamp");

        let timestamp;

        if (get("created_at_unix_timestamp")(value)) {
            timestamp = get("created_at_unix_timestamp")(value);
            // console.log(timestamp);
            return timestamp;
        }

        if (get("utc_unix_time")(value)) {
            let timestamp = get("utc_unix_time")(value);
            // console.log(timestamp);
            return timestamp;
        }

        if (get("utc_iso_datetime")(value)) {
            let timestamp = pipe(get("utc_unix_time"), (value) => moment(value).unix())(value);
            // console.log(timestamp);
            return timestamp;
        }

        timestamp = get("unix_datetime")(value);
        // console.log(timestamp);

        if (!timestamp) {
            console.log("notimestamp");
            console.log(value);
        }

        return timestamp;
    },
};

const ClickFunnels = {
    utilities: {
        getDates: (startDate, endDate) => {
            const dates = [];
            let currentDate = startDate;
            const addDays = function (days) {
                const date = new Date(this.valueOf());
                date.setDate(date.getDate() + days);
                return date;
            };
            while (currentDate <= endDate) {
                dates.push(currentDate);
                currentDate = addDays.call(currentDate, 1);
            }
            return dates;
        },

        get_dates_range_array: (since, until) => {
            let start_date = pipe(
                split("-"),
                map(toNumber),
                mod(1)((value) => value - 1),
                (value) => new Date(...value)
            )(since);

            let end_date = pipe(
                split("-"),
                map(toNumber),
                mod(1)((value) => value - 1),
                (value) => new Date(...value)
            )(until);

            const dates = pipe(
                ([start_date, end_date]) => Rules.utilities.getDates(start_date, end_date),
                mod(all)((date) => moment(date, "YYYY-MM-DD").format("YYYY-MM-DD"))
            )([start_date, end_date]);

            return dates;
        },

        date_pacific_time: (date, timezone = "America/Los_Angeles") => moment(date).tz(timezone),

        date_start_end_timestamps: (
            start = moment().format("YYYY-MM-DD"),
            end = moment().format("YYYY-MM-DD"),
            timezone = "America/Los_Angeles"
        ) => ({
            start: moment(ClickFunnels.utilities.date_pacific_time(start, timezone)).add(1, "days").startOf("day").valueOf(),
            end: moment(ClickFunnels.utilities.date_pacific_time(end, timezone)).add(1, "days").endOf("day").valueOf(),
        }),

        queryDocs: (snapshot) => snapshot.docs.map((doc) => doc.data()),

        rxreducer: rxreduce((prev, curr) => [...prev, ...curr]),

        has_ad_id: anyPass([
            hasPath(["fb_ad_id"]),
            hasPath(["additional_info", "fb_ad_id"]),
            hasPath(["h_ad_id"]),
            hasPath(["additional_info", "h_ad_id"]),
            hasPath(["fb_id"]),
            hasPath(["additional_info", "fb_id"]),
        ]),
    },

    events: {
        orders: (events) => {
            let func_name = "ClickFunnels:events:orders";
            console.log(func_name);

            return rxof(events).pipe(
                // rxmap(pipeLog),
                rxmap((events) => {
                    let profiles = pipe(get(all, "contact_profile"), compact)(events);
                    let first_name = pipe(get(all, "first_name"), head, defaultTo(""))(profiles);
                    let last_name = pipe(get(all, "last_name"), head, defaultTo(""))(profiles);
                    let email = pipe(get(all, "email"), head, defaultTo(""))(profiles);
                    let lower_case_email = toLower(email);

                    let line_items = pipe(
                        get(all, "products"),
                        flatten,
                        compact,
                        louniqby("id"),
                        mod(all)((item) => ({ price: item.amount.cents / 100, name: item.name }))
                    )(events);

                    return {
                        first_name,
                        last_name,
                        email,
                        line_items,
                        lower_case_email,
                    };
                })
            );
        },

        fb_ad_id: {
            paths: (event) =>
                pipe(
                    paths([
                        ["fb_ad_id"],
                        ["additional_info", "fb_ad_id"],
                        ["h_ad_id"],
                        ["additional_info", "h_ad_id"],
                        ["fb_id"],
                        ["additional_info", "fb_id"],
                    ])
                )(event),
            get: (event) => pipe(ClickFunnels.events.fb_ad_id.paths)(event),
        },

        ip: {
            paths: (event) => pipe(paths([["ip"], ["contact", "ip"]]))(event),
            get: (event) => pipe(ClickFunnels.events.ip.paths)(event),
        },
    },

    users: {
        date: {
            events: (date = moment().format("YYYY-MM-DD"), roas_user_id) => {
                let func_name = "ClickFunnels:users:date:events";
                console.log(func_name);

                let { start, end } = ClickFunnels.utilities.date_start_end_timestamps(date, date, "America/Los_Angeles");
                start = Number(moment(start, "x").format("X"));
                end = Number(moment(end, "x").format("X"));

                console.log("roas_user_id", roas_user_id);
                console.log("start", start);
                console.log("end", end);

                return from(
                    getDocs(
                        query(
                            collection(db, "clickfunnels"),
                            where("updated_at_unix_timestamp", ">", start),
                            where("updated_at_unix_timestamp", "<", end),
                            where("user_id", "==", roas_user_id)
                        )
                    )
                ).pipe(rxmap(ClickFunnels.utilities.queryDocs), rxmap(logroupby("email")));
            },
        },
    },

    user: {
        get: curry((prop, value) => {
            let q = query(collection(db, "clickfunnels"), where(prop, "==", value));
            return from(getDocs(q)).pipe(rxmap(ClickFunnels.utilities.queryDocs));
        }),

        events: (email) => {
            let func_name = "ClickFunnels:user:events";
            console.log(func_name);
            return from(getDocs(query(collection(db, "clickfunnels"), where("email", "==", email)))).pipe(rxmap(ClickFunnels.utilities.queryDocs));
        },

        orders: (date = moment().format("YYYY-MM-DD"), email) => {
            let func_name = "ClickFunnels:user:orders";
            console.log(func_name);

            let { start, end } = ClickFunnels.utilities.date_start_end_timestamps(date, date, "America/Los_Angeles");
            start = Number(moment(start, "x").format("X"));
            end = Number(moment(end, "x").format("X"));

            return ClickFunnels.user
                .events(email)
                .pipe(
                    rxmap(get(matching({ updated_at_unix_timestamp: (timestamp) => timestamp > start && timestamp < end }))),
                    concatMap(ClickFunnels.events.orders)
                );
        },
    },

    order: {
        stats: {
            get: (cart) => {
                let func_name = "ClickFunnels:order:stats:get";
                console.log(func_name);

                return { roassales: size(cart), roasrevenue: pipe(get(all, "price"), sum)(cart) };
            },
        },

        ads: {
            ids: ({ type, order, shopping_cart_id }) => {
                let func_name = `Keap:order:ads:get`;
                console.log(func_name);

                if (!order) return throwError(`error:${func_name}:no order`);
                if (!shopping_cart_id) return throwError(`error:${func_name}:no shopping_cart_id`);

                let { email, lower_case_email, roas_user_id } = order;

                return Keap.integrations.webhooks[shopping_cart_id]["user"]["ip"]({ type, email, lower_case_email, roas_user_id }).pipe(
                    concatMap(identity),
                    concatMap(Keap.order.events.get),
                    concatMap(identity),
                    rxmap((event) => ({
                        ipv4: Event.ip.v4(event),
                        ipv6: Event.ip.v6(event),
                        ad_id: Event.ad.id(event),
                        user_agent: Event.user_agent(event),
                        timestamp: Math.trunc(Timestamp.toUTCDigit(Math.trunc(Event.get_utc_timestamp(event)))),
                    })),
                    rxmap(of),
                    rxreduce((prev, curr) => [...prev, ...curr]),
                    rxmap(lofilter((event) => !isUndefined(event.ad_id))),
                    rxfilter((ads) => !isEmpty(ads)),
                    rxmap(louniqby("ad_id")),
                    rxmap(loorderby(["timestamp"], ["desc"])),
                    defaultIfEmpty([])
                );
            },
        },

        events: {
            get: (order) => {
                let func_name = `Keap:order:events:get`;
                console.log(func_name);

                let ipv4_events = Events.user.get.ipv4({ ip: order.ip_address, roas_user_id: order.roas_user_id });
                let ipv6_events = Events.user.get.ipv6({ ip: order.ip_address, roas_user_id: order.roas_user_id });
                return zip([ipv4_events, ipv6_events]).pipe(
                    rxmap(([ipv4, ipv6]) => [...ipv4, ...ipv6]),
                    defaultIfEmpty([])
                );
            },
        },
    },

    report: {
        get: ({ user_id, date, fb_ad_account_id }) => {
            let func_name = `ClickFunnels:report:get`;
            console.log(func_name);

            let orders = ClickFunnels.users.date.events(date, user_id).pipe(
                // rxmap(pipeLog)
                rxmap(values),
                concatMap(identity),
                concatMap(ClickFunnels.events.orders),
                rxmap(of),
                rxreduce((prev, curr) => [...prev, ...curr]),
                rxmap(get(matching({ line_items: (items) => !isEmpty(items) }))),
                rxmap(mod(all)((customer) => ({ ...customer, roas_user_id: user_id }))),
                concatMap(identity),
                rxmap(({ line_items, ...order }) => ({
                    ...order,
                    cart: line_items,
                    stats: ClickFunnels.order.stats.get(line_items),
                })),
                rxmap(pipeLog),
                rxmap(of),
                ClickFunnels.utilities.rxreducer
            );

            // return orders;

            let customers_from_cf = from(orders).pipe(
                rxmap(identity),
                concatMap(identity),
                concatMap((customer) =>
                    from(ClickFunnels.user.get("email", customer.lower_case_email)).pipe(
                        concatMap(identity),
                        rxmap((cfevent) => ({
                            ad_id: pipe(
                                compact,
                                uniq,
                                reject((id) => id == "%7B%7Bad.id%7D%7D" || id == "{{ad.id}}"),
                                head
                            )(ClickFunnels.events.fb_ad_id.get(cfevent)),
                            timestamp: pipe(get("updated_at_unix_timestamp"))(cfevent),
                            ip: pipe(ClickFunnels.events.ip.get, compact, uniq)(cfevent),
                        })),
                        // rxmap(pipeLog),
                        rxmap(of),
                        ClickFunnels.utilities.rxreducer,
                        rxmap(loorderby(["timesamp"], ["desc"])),
                        rxmap(get(matching({ ad_id: (id) => !isEmpty(id) }))),
                        rxmap(louniqby("ad_id")),
                        rxmap(louniqby("timestamp")),
                        rxmap((ads) => ({ ...customer, ads })),
                        rxmap(of)
                    )
                ),
                ClickFunnels.utilities.rxreducer
                // rxmap(pipeLog)
            );

            customers_from_cf_with_ad_ids = customers_from_cf.pipe(rxmap(get(matching({ ads: (ads) => !isEmpty(ads) }))));
            customers_from_cf_without_ad_ids = customers_from_cf.pipe(rxmap(get(matching({ ads: (ads) => isEmpty(ads) }))));

            // return customers_from_cf_without_ad_ids;

            let customers_from_db_events = customers_from_cf_without_ad_ids.pipe(
                concatMap(identity),
                concatMap((customer) =>
                    from(ClickFunnels.user.get("email", customer.lower_case_email)).pipe(
                        concatMap(identity),
                        rxmap(ClickFunnels.events.ip.get),
                        ClickFunnels.utilities.rxreducer,
                        rxmap(pipe(compact, uniq)),
                        concatMap(identity),
                        concatMap((ip_address) => {
                            return zip([from(ipEvents("ipv4", ip_address, user_id)), from(ipEvents("ipv6", ip_address, user_id))]).pipe(
                                rxmap(flatten),
                                concatMap(identity),
                                rxmap(paths([["ipv4"], ["ipv6"]])),
                                rxmap((ips) => [...ips, ip_address]),
                                rxmap(uniq)
                            );
                        }),
                        ClickFunnels.utilities.rxreducer,
                        rxmap(uniq),
                        rxmap(compact),
                        concatMap(identity),
                        concatMap((ip_address) =>
                            zip([from(ipEvents("ipv4", ip_address, user_id)), from(ipEvents("ipv6", ip_address, user_id))]).pipe(
                                rxmap(flatten),
                                tap((value) => console.log("size ->", size(value))),
                                rxmap(pipe(lofilter(ClickFunnels.utilities.has_ad_id))),
                                tap((value) => console.log("size <- ", size(value))),
                                concatMap(identity),
                                rxmap((event) => ({
                                    ad_id: pipe(
                                        paths([["fb_ad_id"], ["h_ad_id"], ["fb_id"], ["ad_id"]]),
                                        compact,
                                        uniq,
                                        reject((id) => id == "%7B%7Bad.id%7D%7D" || id == "{{ad.id}}"),
                                        head
                                    )(event),
                                    timestamp: pipe(Event.get_utc_timestamp)(event),
                                    ip: ip_address,
                                })),
                                rxmap(of),
                                ClickFunnels.utilities.rxreducer,
                                rxmap(loorderby(["timesamp"], ["desc"])),
                                rxmap(get(matching({ ad_id: (id) => !isEmpty(id) })))
                            )
                        ),
                        ClickFunnels.utilities.rxreducer,
                        rxmap(louniqby("ad_id")),
                        rxmap(louniqby("timestamp")),
                        rxmap((ads) => ({ ...customer, ads })),
                        rxmap(of)
                    )
                ),
                ClickFunnels.utilities.rxreducer
                // rxmap(pipeLog)
            );

            // return customers_from_db_events;

            let customers_from_db_events_with_ips = customers_from_db_events.pipe(rxmap(get(matching({ ads: (ads) => !isEmpty(ads) }))));
            let customers_from_db_events_without_ips = customers_from_db_events.pipe(rxmap(get(matching({ ads: (ads) => isEmpty(ads) }))));

            return zip([customers_from_cf_with_ad_ids, customers_from_db_events_with_ips, customers_from_db_events_without_ips]).pipe(
                rxmap(flattenDeep),
                concatMap(identity),
                concatMap((order) => {
                    let { ads, email } = order;
                    let ad_ids = pipe(mod(all)(pick(["ad_id"])))(ads);
                    let ad_details = Facebook.ads.details.get({ ad_ids, fb_ad_account_id, user_id, date }).pipe(
                        rxfilter((ad) => !isUndefined(ad.asset_id)),
                        rxmap((ad) => ({
                            ...ad,
                            email,
                            // ipv4: pipe(get(matching({ ad_id: ad.ad_id }), "ipv4"), head)(ads),
                            // ipv6: pipe(get(matching({ ad_id: ad.ad_id }), "ipv6"), head)(ads),
                            // user_agent: pipe(get(matching({ ad_id: ad.ad_id }), "user_agent"), head)(ads),
                            timestamp: pipe(get(matching({ ad_id: ad.ad_id }), "timestamp"), head)(ads),
                        })),
                        rxmap(of),
                        rxreduce((prev, curr) => [...prev, ...curr]),
                        defaultIfEmpty([])
                    );

                    return from(ad_details).pipe(
                        rxmap((ads) => ({ ...order, ads, email })),
                        defaultIfEmpty({ ...order, ads: [], email })
                    );
                }),
                rxmap(pick(["email", "cart", "ads", "stats", "lower_case_email"])),
                rxmap(of),
                rxreduce((prev, curr) => [...prev, ...curr]),
                rxmap(get(matching({ ads: (ads) => !isEmpty(ads) }))),
                rxmap(pipe(logroupby("lower_case_email"), mod(all)(mergeAll))),
                rxmap((customers) => ({ customers })),
                rxmap((customers) => ({
                    ...customers,
                    date,
                    user_id,
                })),
                catchError((error) => {
                    console.log("Keap:report:get:error");
                    console.log(error);
                    return rxof(error);
                }),
                defaultIfEmpty({ date, customers: {}, user_id })
            );
        },
    },
};

exports.ClickFunnels = ClickFunnels;

let date = "2022-06-01";

let user_id = "FFDSye2jGTT8ky9tHu7X20zXYFJ2";

from(getDocs(query(collection(db, "projects"), where("roas_user_id", "==", user_id)))).pipe(
    rxmap(ClickFunnels.utilities.queryDocs),
    rxmap(lofilter((project) => project.shopping_cart_name !== undefined)),
    rxmap(head),
    concatMap((project) => {
        return from(
            getDocs(query(collectionGroup(db, "integrations"), where("account_name", "==", "facebook"), where("user_id", "==", user_id)))
        ).pipe(
            rxmap(ClickFunnels.utilities.queryDocs),
            rxmap(head),
            rxmap((facebook) => ({ ...project }))
        );
    })
);
// .subscribe((project) => {
//     console.log("project");
//     console.log(project);

//     let { roas_user_id: user_id, fb_ad_account_id, payment_processor_id, shopping_cart_id } = project;
//     let payload = { user_id, fb_ad_account_id, payment_processor_id, shopping_cart_id, date };

//     ClickFunnels.report.get(payload).subscribe((result) => {
//         console.log("result");
//         pipeLog(result);
//     });
// });

from(getDocs(query(collection(db, "events"), where("roas_user_id", "==", user_id), limit(1000)))).pipe(rxmap(ClickFunnels.utilities.queryDocs));
// .subscribe(pipeLog);

from(getDocs(query(collection(db, "clickfunnels"), where("roas_user_id", "==", user_id), limit(1)))).pipe(rxmap(ClickFunnels.utilities.queryDocs));
// .subscribe(pipeLog);
