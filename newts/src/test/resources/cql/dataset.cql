
CREATE TABLE newts.samples (
    partition int,
    resource text,
    collected_at timestamp,
    metric_name text,
    value blob,
    attributes map<text, text>,
    PRIMARY KEY((partition, resource), collected_at, metric_name)
);

CREATE TABLE newts.terms (
    context text,
    field text,
    value text,
    resource text,
    PRIMARY KEY((context, field, value), resource)
);

CREATE TABLE newts.resource_attributes (
    context text,
    resource text,
    attribute text,
    value text,
    PRIMARY KEY((context, resource), attribute)
);

CREATE TABLE newts.resource_metrics (
    context text,
    resource text,
    metric_name text,
    PRIMARY KEY((context, resource), metric_name)
);
