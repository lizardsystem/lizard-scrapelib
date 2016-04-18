import datetime
import os

from lxml import etree
from lxml import builder


def header(type="instantaneous", moduleInstanceId=None,
                          locationId=None, parameterId=None,
                          timeStep_unit="nonequidistant", timeStep_multiplier=None,
                          missVal=-999.0, stationName=None, lat=None, lon=None,
                          units=None, **kwargs):
    if any(x is None for x in
           (locationId, parameterId, stationName, lat, lon, units)):
        raise TypeError("One of the parameters locationId, parameterId, "
                        "stationName, lat, lon, units is not given")
    timeStep = {"unit": timeStep_unit}
    if timeStep_multiplier:
        timeStep.update({"multiplier": timeStep_multiplier})
    kwargs.update({
        "type": type, "timeStep": timeStep, "missVal": missVal,
        "locationId": locationId, "parameterId": parameterId,
        "stationName": stationName, "lat": lat, "lon": lon, "units": units,
        "startDate": None, "endDate": None
    })
    if moduleInstanceId:
        kwargs.update({"moduleInstanceId": moduleInstanceId})
    return kwargs


def write_xml_to_file(filename, tree):
    with open(filename, 'a') as f:
        f.write(
            etree.tostring(
                tree, pretty_print=True, xml_declaration=True, encoding='utf-8'
            ).decode('utf-8')
        )


def create(headerdicts, values, filename="pixml_for_lizard.xml", timeZone=0.0):
    """
    Args:
        values(iterable): [(date_time, value, flag), ...]
        headerdicts(iterable): [{*}, ...]
            * one of the Headerelements below with a value
    """
    schema = "http://www.wldelft.nl/fews/PI"
    xsi = "http://www.w3.org/2001/XMLSchema-instance"
    Element = builder.ElementMaker(nsmap={None: schema, 'xsi': xsi})
    TimeZone = Element.timeZone
    Root = Element.TimeSeries
    Event = Element.event
    Series = Element.series
    Header = Element.header
    Headerelements = {
        'type': Element.type,
        'moduleInstanceId': Element.moduleInstanceId,
        'locationId': Element.locationId,
        'parameterId': Element.parameterId,
        'timeStep': Element.timeStep,
        'startDate': Element.startDate,
        'endDate': Element.endDate,
        'missVal': Element.missVal,
        'stationName': Element.stationName,
        'lat': Element.lat,
        'lon': Element.lon,
        'x': Element.x,
        'y': Element.y,
        'units': Element.units
    }

    order = ["type", "moduleInstanceId", "locationId", "parameterId",
             "timeStep", "startDate", "endDate", "missVal", "stationName",
             "lat", "lon", "units"]
    header_order = [
        x for x in order if x in next(iter(headerdicts.values()))]
    for key in list(values.keys()):
        valueelements = values[key]
        headerelements = headerdicts[key]
        print('processing', key)
        event_elements = []
        min_date = datetime.datetime.now()
        max_date = datetime.datetime(1, 1, 1)
        for value in valueelements:
            min_date = min(min_date, value["datetime"])
            max_date = max(max_date, value["datetime"])
            date, time = value["datetime"].strftime(
                '%Y-%m-%d %H:%M:%S').split(' ')
            event_elements.append(Event(date=date,
                                        time=time,
                                        value=str(value["value"]),
                                        flag=str(value["flag"])))
        header_elements = []
        for name in header_order:
            value = None
            if name == "startDate":
                date, time = min_date.strftime('%Y-%m-%d %H:%M:%S').split(' ')
                val_dict = {"date": date, "time": time}
            elif name == "endDate":
                date, time = max_date.strftime('%Y-%m-%d %H:%M:%S').split(' ')
                val_dict = {"date": date, "time": time}
            elif name == "timeStep":
                val_dict = headerelements[name]
            else:
                value = str(headerelements[name])
            if value:
                header_elements.append(Headerelements[name](value))
            else:
                header_elements.append(Headerelements[name](**val_dict))
        header = Header(*header_elements)
        series = Series(header, *event_elements)
        print('removing {}'.format(key))
        del values[key]
        del headerdicts[key]
        write_xml_to_file(filename + '.tmp', series)
    timeZone = TimeZone(str(timeZone))
    root = Root(timeZone)
    root.attrib['{{{pre}}}schemaLocation'.format(pre=xsi)] = \
        "http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/" \
        "version1.0/pi-schemas/pi_timeseries.xsd"
    root.attrib['version'] = "1.17"
    root_string = etree.tostring(
        root, pretty_print=True, xml_declaration=True, encoding='utf-8'
    ).decode('utf-8')
    begin = '\n'.join(root_string.split('\n')[:3])
    end = '\n'.join(root_string.split('\n')[3:])
    with open(filename, 'w') as f:
        f.write(begin)
        with open(filename + '.tmp', 'r') as f2:
            for line in f2:
                f.write(line)
        f.write(end)
    os.remove(filename + '.tmp')



