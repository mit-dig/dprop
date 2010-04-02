import json

if 'JSONEncoder' not in json.__dict__:
    from types import DictType, ListType

class Nothing:
    """Nothing is known of a value (this is prior to explicitly setting 
    the value)."""
    def __iter__(self):
        # Autofail.
        class NothingIterator:
            def __iter__(self):
                return self
            
            def next(self):
                raise StopIteration
        
        return NothingIterator()
    
    def keys(self):
        return []

class URI:
    """A URI reference."""
    def __init__(self, value):
        self.value = value

if 'JSONEncoder' in json.__dict__:
    class DPropEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, Nothing):
                return {'__nothing__': True}
            elif isinstance(obj, URI):
                return {'__uri__': True,
                        'uri': obj.value}
            return json.JSONEncoder.default(self, obj)
else:
    def DPropEncode(obj):
        if isinstance(obj, Nothing):
            return {'__nothing__': True}
        elif isinstance(obj, URI):
            return {'__uri__': True,
                    'uri': obj.value}
        return obj

def DPropDecode(dct):
    if '__nothing__' in dct:
        return Nothing()
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
