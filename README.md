# Snappy


## Creating driver
A driver MUST accept any number of arguments for registered handlers, for example:

```python
    def create(self, *args, **kwargs):
        return True, ''
```

If this is not honored, the driver may crash and cause disruptions in the agent.

For the return of any registered function, it MUST return a tuple of type (bool, string), if
you wish to stop any further execution of the agent, return an exception of the type SnappyDriverException
located in snappy.exceptions

 ( Does not exists yet)

```python
from snappy.exceptions import SnappyDriverException

    def create(self, *args, **kwargs):
        if something_went_booboo:
            raise SnappyDriverException(1234)
        
        return True, ''
```
