#!/usr/bin/python
# -*- coding: utf-8 -*-
#

d_begin = "2013-01-01"
d_end   = "2013-12-31"
n_peoples = 621700
factor = 100000.0/n_peoples

s_sql1 = """SELECT count(id), MONTH(visit_date) FROM mis.em$tickets 
        WHERE visit_date >= %s AND visit_date <= %s 
        GROUP BY MONTH(visit_date);"""    


# http://matplotlib.org/examples/api/radar_chart.html
# https://gist.github.com/dschien/8805271
#
"""
Example of creating a radar chart (a.k.a. a spider or star chart) [1]_.

Although this example allows a frame of either 'circle' or 'polygon', polygon
frames don't have proper gridlines (the lines are circles instead of polygons).
It's possible to get a polygon grid by setting GRIDLINE_INTERPOLATION_STEPS in
matplotlib.axis to the desired number of vertices, but the orientation of the
polygon is not aligned with the radial axes.

.. [1] http://en.wikipedia.org/wiki/Radar_chart
"""
import numpy as np

import matplotlib.pyplot as plt
from matplotlib.path import Path
from matplotlib.spines import Spine
from matplotlib.projections.polar import PolarAxes
from matplotlib.projections import register_projection


def radar_factory(num_vars, frame='circle'):
    """Create a radar chart with `num_vars` axes.

    This function creates a RadarAxes projection and registers it.

    Parameters
    ----------
    num_vars : int
        Number of variables for radar chart.
    frame : {'circle' | 'polygon'}
        Shape of frame surrounding axes.

    """
    # calculate evenly-spaced axis angles
    theta = 2*np.pi * np.linspace(0, 1-1./num_vars, num_vars)
    # rotate theta such that the first axis is at the top
    theta += np.pi/2

    def draw_poly_patch(self):
        verts = unit_poly_verts(theta)
        return plt.Polygon(verts, closed=True, edgecolor='k')

    def draw_circle_patch(self):
        # unit circle centered on (0.5, 0.5)
        return plt.Circle((0.5, 0.5), 0.5)

    patch_dict = {'polygon': draw_poly_patch, 'circle': draw_circle_patch}
    if frame not in patch_dict:
        raise ValueError('unknown value for `frame`: %s' % frame)

    class RadarAxes(PolarAxes):

        name = 'radar'
        # use 1 line segment to connect specified points
        RESOLUTION = 1
        # define draw_frame method
        draw_patch = patch_dict[frame]

        def fill(self, *args, **kwargs):
            """Override fill so that line is closed by default"""
            closed = kwargs.pop('closed', True)
            return super(RadarAxes, self).fill(closed=closed, *args, **kwargs)

        def plot(self, *args, **kwargs):
            """Override plot so that line is closed by default"""
            lines = super(RadarAxes, self).plot(*args, **kwargs)
            for line in lines:
                self._close_line(line)

        def _close_line(self, line):
            x, y = line.get_data()
            # FIXME: markers at x[0], y[0] get doubled-up
            if x[0] != x[-1]:
                x = np.concatenate((x, [x[0]]))
                y = np.concatenate((y, [y[0]]))
                line.set_data(x, y)

        def set_varlabels(self, labels):
            self.set_thetagrids(theta * 180/np.pi, labels)

        def _gen_axes_patch(self):
            return self.draw_patch()

        def _gen_axes_spines(self):
            if frame == 'circle':
                return PolarAxes._gen_axes_spines(self)
            # The following is a hack to get the spines (i.e. the axes frame)
            # to draw correctly for a polygon frame.

            # spine_type must be 'left', 'right', 'top', 'bottom', or `circle`.
            spine_type = 'circle'
            verts = unit_poly_verts(theta)
            # close off polygon by repeating first vertex
            verts.append(verts[0])
            path = Path(verts)

            spine = Spine(self, spine_type, path)
            spine.set_transform(self.transAxes)
            return {'polar': spine}

    register_projection(RadarAxes)
    return theta


def unit_poly_verts(theta):
    """Return vertices of polygon for subplot axes.

    This polygon is circumscribed by a unit circle centered at (0.5, 0.5)
    """
    x0, y0, r = [0.5] * 3
    verts = [(r*np.cos(t) + x0, r*np.sin(t) + y0) for t in theta]
    return verts

def get_data():
    from dbmysql_connect import DBMY
    
    dbmy = DBMY()
    curr = dbmy.con.cursor()

    curr.execute(s_sql1, (d_begin, d_end, ))

    recs = curr.fetchall()

    m = []
    for rec in recs:
	n = rec[0]
	f = n*factor
	d = rec[1]
	m.append(f)
	
    dbmy.close()
    return m
        
if __name__ == '__main__':
    # http://s.arboreus.com/2009/04/cyrillic-letters-in-matplotlibpylab.html
    from matplotlib import rc

    font = {'family': 'Droid Sans',
	    'weight': 'normal',
	    'size': 13}
    
    rc('font', **font)

    N = 12
    theta = radar_factory(N, frame='polygon')

    spoke_labels = [u'Январь', u'Февраль', u'Март', u'Апрель', 
                    u'Май', u'Июнь', u'Июль', u'Август',
                    u'Сентябрь', u'Октябрь', u'Ноябрь', u'Декабрь']

    a = get_data()
    #print a
    
    b = [0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.]
    
    fig = plt.figure(figsize=(9, 9))
    # adjust spacing around the subplots
    fig.subplots_adjust(wspace=0.2, hspace=0.2, top=0.85, bottom=0.05)
    title_list = ['Intensity']
    data = {'Intensity': [a, b]}
    name = {'Intensity': u'Интенсивность'}
    colors = ['b', 'r']

    radial_grid = [100., 200., 300.]
    # If you don't care about the order, you can loop over data_dict.items()
    for n, title in enumerate(title_list):
        ax = fig.add_subplot(1, 1, n + 1, projection='radar')
        plt.rgrids(radial_grid)
        ax.set_title(name[title], weight='bold', size='medium', position=(0.5, 1.1),
                     horizontalalignment='center', verticalalignment='center')
        for d, color in zip(data[title], colors):
            ax.plot(theta, d, color=color)
            ax.fill(theta, d, facecolor=color, alpha=0.25)
            ax.set_varlabels(spoke_labels)
    # add legend relative to top-left plot
    plt.subplot(1, 1, 1)
    labels = ('2013', '')
    legend = plt.legend(labels, loc=(0.9, .95), labelspacing=0.1)
    plt.setp(legend.get_texts(), fontsize='small')
    plt.figtext(0.5, 0.965, u'Сезонность заболеваемости',
    ha='center', color='black', weight='bold', size='large')
    plt.show()