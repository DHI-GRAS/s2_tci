class GeneratorWithLength:
    _generator = None
    _length = None

    def __init__(self, *args, **kwargs):
        if self._generator is None:
            raise ValueError('_generator property must be set')

        if self._length is None:
            raise ValueError('_length property must be set')

    def __len__(self):
        return self._length

    def __iter__(self):
        return self._generator().__iter__()

    def __next__(self):
        return self._generator().__next__()
