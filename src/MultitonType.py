import json

class MultitonType(type):
    _instances = {}  # a dictionary of all the instances of the class
    _instance_key = None  # the key used to identify the instance

    # factory method to create a new instance of the class with the instance key specified
    @classmethod
    def with_key(cls, _instance_key) -> type:
        return type(cls.__name__, (cls,), {'_instance_key': _instance_key})

    # main functionality for MultitonType behavior
    @classmethod
    def __call__(cls, *args, **kwargs) -> object:
        # args and kwargs are the arguments passed to the class

        # create a string representation of the class
        if cls._instance_key is None:
            instance_key: str = cls.__name__
        else:
            if len(args) > 0:
                instance_key: str = cls.__name__ + json.dumps(str(args[0]), sort_keys=True)
            elif len(kwargs) > 0:
                if cls._instance_key not in kwargs:
                    raise ValueError("A valid 'key' must be provided")
                instance_key: str = cls.__name__ + json.dumps(str(kwargs[cls._instance_key]), sort_keys=True)
            else:
                instance_key: str = cls.__name__

        # if the class is not in the instance dictionary, add it using the representation as the key
        if instance_key not in cls._instances:
            cls._instances[instance_key] = super(MultitonType, cls).__call__(*args, **kwargs)

        # return the instance of the class
        return cls._instances[instance_key]
