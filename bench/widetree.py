import hotshot
import hotshot.stats

import unittest
import os
import tempfile

from tables import *

verbose = 0


class WideTreeTestCase(unittest.TestCase):
    """Checks for maximum number of childs for a Group."""

    def test00_Leafs(self):
        """Checking creation of large number of leafs (1024) per group.

        Variable 'maxchilds' controls this check. PyTables support up to
        4096 childs per group, but this would take too much memory (up
        to 64 MB) for testing purposes (may be we can add a test for big
        platforms). A 1024 childs run takes up to 30 MB. A 512 childs
        test takes around 25 MB.

        """

        import time
        maxchilds = 1000
        if verbose:
            print('\n', '-=' * 30)
            print(f"Running {self.__class__.__name__}.test00_wideTree...")
            print(f"Maximum number of childs tested : {maxchilds}")
        # Open a new empty HDF5 file
        #file = tempfile.mktemp(".h5")
        file = "test_widetree.h5"

        fileh = open_file(file, mode="w")
        if verbose:
            print("Children writing progress: ", end=' ')
        for child in range(maxchilds):
            if verbose:
                print(f"{child:3d},", end=' ')
            a = [1, 1]
            fileh.create_group(fileh.root, f"group{child}",
                               f"child: {child}")
            fileh.create_array(f"/group{child}", f'array{child}',
                               a, f"child: {child}")
        if verbose:
            print()
        # Close the file
        fileh.close()

        t1 = time.time()
        # Open the previous HDF5 file in read-only mode
        fileh = open_file(file, mode="r")
        print(
            f"\nTime spent opening a file with {maxchilds} groups + "
            f"{maxchilds} arrays: {time.time() - t1} s")
        if verbose:
            print("\nChildren reading progress: ", end=' ')
        # Close the file
        fileh.close()
        # Then, delete the file
        # os.remove(file)

    def test01_wideTree(self):
        """Checking creation of large number of groups (1024) per group.

        Variable 'maxchilds' controls this check. PyTables support up to
        4096 childs per group, but this would take too much memory (up
        to 64 MB) for testing purposes (may be we can add a test for big
        platforms). A 1024 childs run takes up to 30 MB. A 512 childs
        test takes around 25 MB.

        """

        import time
        maxchilds = 1000
        if verbose:
            print('\n', '-=' * 30)
            print(f"Running {self.__class__.__name__}.test00_wideTree...")
            print(f"Maximum number of childs tested : {maxchilds}")
        # Open a new empty HDF5 file
        file = tempfile.mktemp(".h5")
        #file = "test_widetree.h5"

        fileh = open_file(file, mode="w")
        if verbose:
            print("Children writing progress: ", end=' ')
        for child in range(maxchilds):
            if verbose:
                print(f"{child:3d},", end=' ')
            fileh.create_group(fileh.root, f"group{child}", f"child: {child}")
        if verbose:
            print()
        # Close the file
        fileh.close()

        t1 = time.time()
        # Open the previous HDF5 file in read-only mode
        fileh = open_file(file, mode="r")
        print(f"\nTime spent opening a file with {maxchilds} groups: {time.time() - t1} s")
        # Close the file
        fileh.close()
        # Then, delete the file
        os.remove(file)

#----------------------------------------------------------------------


def suite():
    theSuite = unittest.TestSuite()
    theSuite.addTest(unittest.makeSuite(WideTreeTestCase))

    return theSuite


if __name__ == '__main__':
    prof = hotshot.Profile("widetree.prof")
    benchtime, stones = prof.runcall(unittest.main(defaultTest='suite'))
    prof.close()
    stats = hotshot.stats.load("widetree.prof")
    stats.strip_dirs()
    stats.sort_stats('time', 'calls')
    stats.print_stats(20)
