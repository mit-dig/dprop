import json

if 'JSONEncoder' not in json.__dict__:
    from types import DictType, ListType

class Undefined:
    """A value is undefined (this is prior to explicitly setting the value)."""
    pass

class URI:
    """A URI reference."""
    def __init__(self, value):
        self.value = value

if 'JSONEncoder' in json.__dict__:
    class DPropEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, Undefined):
                return {'__undefined__': True}
            elif isinstance(obj, URI):
                return {'__uri__': True,
                        'uri': obj.value}
            return json.JSONEncoder.default(self, obj)
else:
    def DPropEncode(obj):
        if isinstance(obj, Undefined):
            return {'__undefined__': True}
        elif isinstance(obj, URI):
            return {'__uri__': True,
                    'uri': obj.value}
        return obj

def DPropDecode(dct):
    if '__undefined__' in dct:
        return Undefined()
    elif '__uri__' in dct:
        return URI(dct['uri'])
    return dct

if 'JSONEncoder' in json.__dict__:
    def loads(str):
        return json.loads(str, object_hook=DPropDecode)
else:
    def loads(str):
        def fixDicts(obj):
            if isinstance(obj, DictType):
                for key in obj.keys():
                    obj[key] = fixDicts(obj[key])
                return DPropDecode(obj)
            elif isinstance(obj, ListType):
                for i in range(len(obj)):
                    obj[i] = fixDicts(obj[i])
            return obj
        obj = json.read(str)
        return fixDicts(obj)

if 'JSONEncoder' in json.__dict__:
    def dumps(obj):
        return json.dumps(obj, cls=DPropEncoder)
else:
    def dumps(obj):
        def fixDicts(obj):
            if isinstance(obj, DictType):
                for key in obj.keys():
                    obj[key] = fixDicts(obj[key])
            elif isinstance(obj, ListType):
                for i in range(len(obj)):
                    obj[i] = fixDicts(obj[i])
            return DPropEncode(obj)
        return json.write(fixDicts(obj))
