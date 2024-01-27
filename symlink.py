#! /bin/python3

####################################################
# Title:  symlink.py                               #
# Author: Jakub Štětina <xsteti05@stud.fit.vut.cz> #
# Date:   22 Jan 2023                              #
####################################################

import os
import errno

def symlink(target, link_name):
    """
    Equivalent to `ln -sf target link_name`. 
    """
    try:
        os.symlink(target, link_name)
    except OSError as e:
        if e.errno == errno.EEXIST:
            os.remove(link_name)
            os.symlink(target, link_name)
        else:
            raise e
        
        