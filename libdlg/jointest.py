def join_(self, field, name=None, constraint=None, fields=[], orderby=None):
    if len(self) == 0:
        return self
    mode = 'referencing' if field.type == 'id' else 'referenced'
    func = lambda ids: field.belongs(ids)
    db, ids, maps = self.db, [], {}
    if not fields:
        fields = [f for f in field._table if f.readable]
    if mode == 'referencing':
        # try all refernced field names
        names = [name] if name else list(set(
            f.name for f in field._table._referenced_by if f.name in self[0]))
        # get all the ids
        ids = [row.get(name) for row in self for name in names]
        # filter out the invalid ids
        ids = filter(lambda id: str(id).isdigit(), ids)
        # build the query
        query = func(ids)
        if constraint: query = query & constraint
        tmp = not field.name in [f.name for f in fields]
        if tmp:
            fields.append(field)
        other = db(query).select(*fields, orderby=orderby, cacheable=True)
        for row in other:
            id = row[field.name]
            maps[id] = row
        for row in self:
            for name in names:
                row[name] = maps.get(row[name])
    if mode == 'referenced':
        if not name:
            name = field._tablename
        # build the query
        query = func([row.id for row in self])
        if constraint: query = query & constraint
        name = name or field._tablename
        tmp = not field.name in [f.name for f in fields]
        if tmp:
            fields.append(field)
        other = db(query).select(*fields, orderby=orderby, cacheable=True)
        for row in other:
            id = row[field]
            if not id in maps: maps[id] = []
            if tmp:
                try:
                    del row[field.name]
                except:
                    del row[field.tablename][field.name]
                    if not row[field.tablename] and len(row.keys()) == 2:
                        del row[field.tablename]
                        row = row[row.keys()[0]]
            maps[id].append(row)
        for row in self:
            row[name] = maps.get(row.id, [])
    return self