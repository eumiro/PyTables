import tables


class Particle(tables.IsDescription):
    name = tables.StringCol(16, pos=1)      # 16-character String
    lati = tables.Int32Col(pos=2)           # integer
    longi = tables.Int32Col(pos=3)          # integer
    pressure = tables.Float32Col(pos=4)     # float  (single-precision)
    temperature = tables.Float64Col(pos=5)  # double (double-precision)

# Open a file in "w"rite mode
fileh = tables.open_file("table1.h5", mode="w")
# Create a new group
group = fileh.create_group(fileh.root, "newgroup")

# Create a new table in newgroup group
table = fileh.create_table(group, 'table', Particle, "A table",
                           tables.Filters(1))
particle = table.row

# Fill the table with 10 particles
for i in range(10):
    # First, assign the values to the Particle record
    particle['name'] = f'Particle: {i:6d}'
    particle['lati'] = i
    particle['longi'] = 10 - i
    particle['pressure'] = float(i * i)
    particle['temperature'] = float(i ** 2)
    # This injects the row values.
    particle.append()

# We need to flush the buffers in table in order to get an
# accurate number of records on it.
table.flush()

# Add a couple of user attrs
table.attrs.user_attr1 = 1.023
table.attrs.user_attr2 = "This is the second user attr"

# Append several rows in only one call
table.append([("Particle:     10", 10, 0, 10 * 10, 10 ** 2),
              ("Particle:     11", 11, -1, 11 * 11, 11 ** 2),
              ("Particle:     12", 12, -2, 12 * 12, 12 ** 2)])

group = fileh.root.newgroup
print("Nodes under group", group, ":")
for node in fileh.list_nodes(group):
    print(node)
print()

print("Leaves everywhere in file", fileh.filename, ":")
for leaf in fileh.walk_nodes(classname="Leaf"):
    print(leaf)
print()

table = fileh.root.newgroup.table
print("Object:", table)
print(f"Table name: {table.name}. Table title: {table.title}")
print(f"Rows saved on table: {table.nrows}")

print("Variable names on table with their type:")
for name in table.colnames:
    print(f"   {name} := {table.coldtypes[name]}")

print("Table contents:")
for row in table:
    print(row[:])
print("Associated recarray:")
print(table.read())

# Finally, close the file
fileh.close()
