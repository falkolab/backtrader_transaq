def maybe_name(obj) -> str:
    """ Returns an object's __name__ attribute or it's string representation.
    @param obj any object
    @return obj name or string representation
    """
    try:
        return obj.__name__
    except (AttributeError, ):
        return str(obj)
