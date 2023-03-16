# noinspection
class RecursiveNamespace:
    '''recursive namespace class

    @DynamicAttrs
    '''

    @staticmethod
    def map_entry(entry):
        '''entry 타입에 따라 객체를 반환'''
        if isinstance(entry, dict):
            return RecursiveNamespace(**entry)
        return entry

    def __init__(self, **kwargs):
        '''val 타입에 따라 객체를 반환'''
        for key, val in kwargs.items():
            if type(val) == dict:
                setattr(self, key, RecursiveNamespace(**val))
            elif type(val) == list:
                setattr(self, key, list(map(self.map_entry, val)))
            else:
                setattr(self, key, val)
