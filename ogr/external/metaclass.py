"""
Copyright (c) 2016-2017, John Kirkham, Howard Hughes Medical Institute
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

Source: https://github.com/jakirkham/metawrap
"""


def metaclass(meta):
    """
    Returns a decorator that decorates a class such that the given
    metaclass is applied.

    Note:
        Decorator will add the __metaclass__ attribute so the last
        metaclass applied is known. Also, decorator will add the
        __wrapped__ attribute so that the unwrapped class can be retrieved.

    Args:
        meta(metaclass):     metaclass to apply to a given class.

    Returns:
        (decorator):         a decorator for the class.
    """

    def metaclass_wrapper(cls):
        """
        Returns a decorated class such that the given metaclass is applied.

        Note:
            Adds the __metaclass__ attribute so the last metaclass used is
            known. Also, adds the __wrapped__ attribute so that the
            unwrapped class can be retrieved.

        Args:
            cls(class):          class to decorate.

        Returns:
            (class):             the decorated class.

        """

        __name = str(cls.__name__)
        __bases = tuple(cls.__bases__)
        __dict = dict(cls.__dict__)

        __dict.pop("__dict__", None)
        __dict.pop("__weakref__", None)

        for each_slot in __dict.get("__slots__", tuple()):
            __dict.pop(each_slot, None)

        __dict["__metaclass__"] = meta
        __dict["__wrapped__"] = cls

        return meta(__name, __bases, __dict)

    return metaclass_wrapper
