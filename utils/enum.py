class EnumWrapper:
    def __init__(self, init_enum, name=None, dict_links={}, is_member=False):
        self._init_enum = init_enum
        self._dict_links = dict_links
        self.enum_name = name or init_enum.__name__
        if not is_member:
            for name, member in init_enum.__dict__['_member_map_'].items():
                self.__dict__[name] = EnumWrapperItem(name, member, self)

    def get_enum(self):
        return self._init_enum

    def get_item(self, name):
        item = self.__dict__.get(name)
        if not item:
            item = self.get_enum()(name)
            if item:
                item = EnumWrapper(name, item, self)
        return item

    def get_links(self):
        return self._dict_links

    def get_dict(self, field):
        return self.get_links().get(field)

    def set_dict(self, field, field_dict):
        self._dict_links[field] = field_dict

    def get_prop(self, name, item=None):
        if not item:
            item = self
        dict_prop = self.get_dict(name)
        for f in (item, item.name, item.value):
            value = dict_prop.get(f)
            if value:
                break
        return value

    def get_func(self, name, item=None):
        def func():
            return self.get_prop(name, item)
        return func

    def __call__(self, value):
        if isinstance(value, self.__class__):
            return value
        elif value in self.__dict__:
            return self.__dict__[value]
        else:
            item = self.get_enum()(value)
            return self.__dict__[item.name]


class EnumWrapperItem(EnumWrapper):
    def __init__(self, name, member, parent):
        self.name = name
        self.value = member.value
        self.member = member
        self._parent = parent
        super().__init__(init_enum=member.__class__, is_member=True)

    def get_name(self):
        return self.name

    def get_value(self):
        return self.value

    def __getattribute__(self, name):
        super_dict = super().__dict__
        if name in super_dict:
            return super_dict[name]
        elif name.startswith('_') or name in self.__dir__():
            return super().__getattribute__(name)
        try:
            return super().__getattribute__('_parent').get_item(name)
        except ValueError:
            if name.startswith('get_'):
                return super().__getattribute__('get_func')(name[4:])
            else:
                return super().__getattribute__('get_prop').get(name)
            pass

    def __repr__(self):
        return '{}.{}'.format(self._parent.enum_name, self.get_name())

    def __str__(self):
        return "<{}: '{}'>".format(self.__repr__(), self.get_value())
