import unittest

from tables import *
# Next imports are only necessary for this test suite
#from tables import Group, Leaf, Table, Array

verbose = 0


class Test(IsDescription):
    ngroup = Int32Col(pos=1)
    ntable = Int32Col(pos=2)
    nrow = Int32Col(pos=3)
    #string = StringCol(itemsize=500, pos=4)


class WideTreeTestCase(unittest.TestCase):

    def test00_Leafs(self):

        # Open a new empty HDF5 file
        filename = "test_widetree.h5"
        ngroups = 10
        ntables = 300
        nrows = 10
        complevel = 0
        complib = "lzo"

        print("Writing...")
        # Open a file in "w"rite mode
        fileh = open_file(filename, mode="w", title="PyTables Stress Test")

        for k in range(ngroups):
            # Create the group
            group = fileh.create_group("/", f'group{k:04d}', f"Group {k}")

        fileh.close()

        # Now, create the tables
        rowswritten = 0
        for k in range(ngroups):
            print("Filling tables in group:", k)
            fileh = open_file(filename, mode="a", root_uep=f'group{k:04d}')
            # Get the group
            group = fileh.root
            for j in range(ntables):
                # Create a table
                table = fileh.create_table(group, f'table{j:04d}', Test,
                                           f'Table{j:04d}',
                                           Filters(complevel, complib), nrows)
                # Get the row object associated with the new table
                row = table.row
                # Fill the table
                for i in range(nrows):
                    row['ngroup'] = k
                    row['ntable'] = j
                    row['nrow'] = i
                    row.append()

                rowswritten += nrows
                table.flush()

            # Close the file
            fileh.close()

        # read the file
        print("Reading...")
        rowsread = 0
        for ngroup in range(ngroups):
            fileh = open_file(filename, mode="r", root_uep=f'group{ngroup:04d}')
            # Get the group
            group = fileh.root
            ntable = 0
            if verbose:
                print("Group ==>", group)
            for table in fileh.list_nodes(group, 'Table'):
                if verbose > 1:
                    print(f"Table ==> {table}")
                    print(f"Max rows in buf: {table.nrowsinbuf}")
                    print(f"Rows in {table._v_pathname} : {table.nrows}")
                    print(f"Buffersize: {table.rowsize * table.nrowsinbuf}")
                    print(f"MaxTuples: {table.nrowsinbuf}")

                nrow = 0
                for row in table:
                    try:
                        assert row["ngroup"] == ngroup
                        assert row["ntable"] == ntable
                        assert row["nrow"] == nrow
                    except:
                        print(f"Error in group: {ngroup}, table: {ntable}, row: {nrow}")
                        print(f"Record ==> {row}")
                    nrow += 1

                assert nrow == table.nrows
                rowsread += table.nrows
                ntable += 1

            # Close the file (eventually destroy the extended type)
            fileh.close()


#----------------------------------------------------------------------
def suite():
    theSuite = unittest.TestSuite()
    theSuite.addTest(unittest.makeSuite(WideTreeTestCase))

    return theSuite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
