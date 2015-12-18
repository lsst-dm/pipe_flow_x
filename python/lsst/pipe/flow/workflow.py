"""
workflow
"""
from __future__ import absolute_import, division, print_function
import networkx as nx
from lsst.pipe.base.struct import Struct
from lsst.pipe.base.super_task import SuperTask
import lsst.pex.config as pexConfig



__all__ = ["WorkFlowTask", "WorkFlowSeqTask", "WorkFlowParTask", "WorkFlowConfig"]



class WorkFlowConfig(pexConfig.Config):
    """
    Config for Workflow
    """
    minval = pexConfig.Field(
        dtype=int,
        doc="Min value",
        default=2,
    )



class WorkFlowTask(SuperTask):
    """
    Workflow Generic
    """
    ConfigClass = WorkFlowConfig
    _default_name = 'WorkFlowTask'

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None, input=None):
        super(WorkFlowTask, self).__init__(config, name, parent_task, log, activator)

        self._parser = None
        self.input = Struct()
        self.output = None

    @property
    def parser(self):
        return self._parser

    @parser.setter
    def parser(self, parser):
        self._parser = parser

    def get_tasks_labels(self):
        labels = nx.get_node_attributes(self._subgraph, 'label')
        return labels


    @property
    def get_dot(self):
        """
        get subgrapgh
        :return:
        """
        self.lines=[]

        if self._first is not None:
            if (self._task_kind == 'WorkFlowSeqTask'):
                self._current = self._first
            else:
                self.nodes=self._subgraph.nodes_iter()
                self._current = self.nodes.next()

        if (self._current._task_kind == 'WorkFlowSeqTask') :
            self.lines.append('subgraph cluster_%s {' % self._current.name)
            self.lines.append('label = %s ;' % self._current.name)
            self.lines.append('dummy_%s [shape=point, style=invis];' % self._current.name)
            temp_dot = self._current.get_dot
            for branch in temp_dot:
                self.lines.append(branch)
            for branch in self._current.add_edges():
                self.lines.append(branch)
            self.lines.append('}')
        elif (self._current._task_kind == 'WorkFlowParTask') :
            self.lines.append('subgraph cluster_%s {' % self._current.name)
            self.lines.append('node [shape = doublecircle];')
            self.lines.append('label = %s ;' % self._current.name)
            self.lines.append('dummy_%s [shape=point, style=invis];' % self._current.name)
            temp_dot = self._current.get_dot
            for branch in temp_dot:
                self.lines.append(branch)
        else:
            self.lines.append(self._current.name+';')

        if self._task_kind == 'WorkFlowSeqTask':
            while True:
                if not self._subgraph.successors(self._current):
                    break
                else:
                    self._current = self._subgraph.successors(self._current)[0]
                    if (self._current._task_kind == 'WorkFlowSeqTask') :
                        self.lines.append('subgraph cluster_%s {' % self._current.name)
                        self.lines.append('dummy_%s [shape=point, style=invis];' % self._current.name)
                        self.lines.append('label = %s ;' % self._current.name)
                        temp_dot = self._current.get_dot
                        for branch in temp_dot:
                            self.lines.append(branch)
                        for branch in self._current.add_edges():
                            self.lines.append(branch)
                        self.lines.append('}')
                    elif (self._current._task_kind == 'WorkFlowParTask'):
                        self.lines.append('subgraph cluster_%s {' % self._current.name)
                        self.lines.append('node [shape = doublecircle];')

                        self.lines.append('dummy_%s [shape=point, style=invis];' % self._current.name)
                        self.lines.append('label = %s ;' % self._current.name)
                        temp_dot = self._current.get_dot
                        for branch in temp_dot:
                            self.lines.append(branch)
                        self.lines.append('}')

                    else:
                        self.lines.append(self._current.name+';')
        else:
            while True:
                try:
                    self._current = self.nodes.next()
                except StopIteration:
                    break

                if (self._current._task_kind == 'WorkFlowParTask'):
                    self.lines.append('subgraph cluster_%s {' % self._current.name)
                    self.lines.append('node [shape = doublecircle];')

                    self.lines.append('dummy_%s [shape=point, style=invis];' % self._current.name)
                    self.lines.append('label = %s ;' % self._current.name)
                    temp_dot = self._current.get_dot
                    for branch in temp_dot:
                        self.lines.append(branch)
                    self.lines.append('}')
                elif (self._current._task_kind == 'WorkFlowSeqTask'):
                    self.lines.append('subgraph cluster_%s {' % self._current.name)
                    self.lines.append('dummy_%s [shape=point, style=invis];' % self._current.name)
                    self.lines.append('label = %s ;' % self._current.name)
                    temp_dot = self._current.get_dot
                    for branch in temp_dot:
                        self.lines.append(branch)
                    for branch in self._current.add_edges():
                        self.lines.append(branch)
                    self.lines.append('}')
                else:
                    self.lines.append(self._current.name+';')

        return self.lines

    def write_tree(self):
        """
        Write dot file
        :return:
        """
        lines = ['digraph %s {' % self.name]
        lines.append('rankdir=LR;')
        lines.append('compound=true;')
        lines.append('subgraph cluster_%s {' % self.name)
        lines.append('label = %s ;' % self.name)
        for branch in self.get_dot:
            lines.append(branch)
        if self._task_kind == 'WorkFlowSeqTask':
            for branch in self.add_edges():
                lines.append(branch)
        lines.append('}')
        lines.append('}')
        F=open(self.name+'.dot','w')
        for l in lines:
            F.write(l+' \n')
        F.close()

    def get_tree(self,lasttab=' ',tab='+--> '):
        """
        get tree
        :return:
        """


        self.tree = [lasttab+self.name]
        if self._first is not None:
            if (self._task_kind == 'WorkFlowSeqTask'):
                self._current = self._first
                tab=tab.replace('o','>')
            if (self._task_kind == 'WorkFlowParTask'):
                self.nodes=self._subgraph.nodes_iter()
                self._current = self.nodes.next()
                tab=tab.replace('>','o')

        tab = '|    '+tab

        if self._current._task_kind == 'WorkFlowSeqTask':
            temp_tree = self._current.get_tree(lasttab=tab, tab=tab.replace('o','>'))
            for branch in temp_tree:
                self.tree.append(branch)
        elif self._current._task_kind == 'WorkFlowParTask':
            temp_tree = self._current.get_tree(lasttab=tab, tab=tab.replace('>','o'))
            for branch in temp_tree:
                self.tree.append(branch)
        else:
            self.tree.append(tab+self._current.name)

        if self._task_kind == 'WorkFlowSeqTask':
            while True:
                if not self._subgraph.successors(self._current):
                    break
                else:
                    self._current = self._subgraph.successors(self._current)[0]
                    if self._current._task_kind == 'WorkFlowSeqTask':
                        temp_tree = self._current.get_tree(lasttab=tab, tab=tab.replace('o','>'))
                        for branch in temp_tree:
                            self.tree.append(branch)
                    elif self._current._task_kind == 'WorkFlowParTask':
                        temp_tree = self._current.get_tree(lasttab=tab, tab=tab.replace('>','o'))
                        for branch in temp_tree:
                            self.tree.append(branch)
                    else:
                        self.tree.append(tab+self._current.name)
        if self._task_kind == 'WorkFlowParTask':
            while True:
                try:
                    self._current = self.nodes.next()
                except StopIteration:
                    break
                if self._current._task_kind == 'WorkFlowSeqTask':
                    temp_tree = self._current.get_tree(lasttab=tab, tab=tab.replace('o','>'))
                    for branch in temp_tree:
                        self.tree.append(branch)
                elif self._current._task_kind == 'WorkFlowParTask':
                    temp_tree = self._current.get_tree(lasttab=tab, tab=tab.replace('>','o'))
                    for branch in temp_tree:
                        self.tree.append(branch)
                else:
                    self.tree.append(tab+self._current.name)


        return self.tree


    def print_tree(self):
        """
        Print Tree Ascii
        :return:
        """
        print()
        print('* Workflow Tree * :')
        print()
        for branch in self.get_tree():
            print(branch)
        print()






class WorkFlowSeqTask(WorkFlowTask):
    """
    SuperTask Sequential
    """

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        super(WorkFlowSeqTask, self).__init__(config, name, parent_task, log, activator)

        self._subgraph = nx.DiGraph(label = self.name)

        self._last = None
        self._last_kind = None
        self._first = None
        self._first_kind = None
        self._task_list = dict()
        self._task_kind = 'WorkFlowSeqTask'
        self._tasks_kind = None
        self._current = None
        self._next = None


    def link(self, *subtasks):
        """
        Add task or subtasks to the supertask
        :param subtasks:
        :return:
        """
        for task in subtasks:
            task._parent_name = self.name
            task._activator = self.activator

            if task.name not in self.get_tasks_labels():
                self._subgraph.add_node(task, label = task.name, kind=self._task_kind)
                if self._first is None:
                    self._first = task
                if self._last is None:
                    self._last = task

                if self._last.name != task.name:
                    self._subgraph.add_edge(self._last, task)
                    self._last = task
        return self


    def execute(self, dataRef, *args, **kwargs):
        """
        Run method for supertask, need to check for order
        :return:
        """
        print('I am running %s Using %s activator' % (self.name, self.activator))
        self.log.info("Processing data ID %s" % (dataRef.dataId,))

        if self._first is not None:
            #if self._first.input is not None:
                #self.input = self._first.input
            self._first.execute(dataRef, *args, **kwargs)
            #self.input.mergeItems(self._first.output, *self._first.output.getDict().keys())
        self._current = self._first
        while True:
            if not self._subgraph.successors(self._current):
                break
            else:
                self._current = self._subgraph.successors(self._current)[0]
                #self._current.input = self.input
                self._current.execute(dataRef, *args, **kwargs)
                #self.input.mergeItems(self._current.output, *self._current.output.getDict().keys())

        #self.output = self.input



    def add_edges(self):
        lines=[]
        for e in self._subgraph.edges():
            source = e[0].name
            target = e[1].name
            opt = []
            if (e[0]._task_kind == 'WorkFlowSeqTask') or (e[0]._task_kind == 'WorkFlowParTask'):
                source = 'dummy_'+e[0].name
                opt.append(' ltail = cluster_'+e[0].name)
            if (e[1]._task_kind == 'WorkFlowSeqTask') or (e[1]._task_kind == 'WorkFlowParTask'):
                target = 'dummy_'+e[1].name
                opt.append(' lhead = cluster_'+e[1].name)
            temp_line = source + '->' + target
            if opt:
                temp_line += ' [ '+ ','.join(opt) + ']'
            lines.append(temp_line+';')
        return lines

    def gconf(self, rootN=''):
        self.list_config = []
        rootN += self.name+'.'
        if self._first is not None:
            if self._first.task_kind == 'SuperTask':
                for key,val in self._first.config.iteritems():
                        self.list_config.append(rootN+self._first.name+'.config.'+key+' = '+str(val))
            else:
                temp_lines = self._first.gconf(rootN=rootN)
                for line in temp_lines:
                    self.list_config.append(line)

        self._current = self._first
        while True:
            if not self._subgraph.successors(self._current):
                break
            else:
                self._current = self._subgraph.successors(self._current)[0]
                if self._current.task_kind == 'SuperTask':
                    for key,val in self._current.config.iteritems():
                            self.list_config.append(rootN+self._current.name+'.config.'+key+' = '+str(val))
                else:
                    temp_lines = self._current.gconf(rootN=rootN)
                    for line in temp_lines:
                        self.list_config.append(line)
        return self.list_config




class WorkFlowParTask(WorkFlowTask):
    """
    SuperTask Parallel (undirected)
    """

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        super(WorkFlowParTask, self).__init__(config, name, parent_task, log, activator)

        self._subgraph = nx.Graph(label = self.name)
        self._task_kind = 'WorkFlowParTask'

        self._last = None
        self._first = None
        self._current = None


    def link(self, *subtasks):
        """
        Add task or subtasks to the supertask
        :param subtasks:
        :return:
        """
        for task in subtasks:
            task._parent_name = self.name
            task._activator = self.activator
            if task.name not in self.get_tasks_labels():
                if self._first is None:
                    self._first = task
                self._subgraph.add_node(task, label = task.name, kind=self._task_kind)

        return self


    def execute(self ,dataRef, *args, **kwargs):
        """
        Run method for supertask, need to check for order
        :return:
        """
        print('I am running %s Using %s activator' % (self.name, self.activator))
        self.log.info("Processing data ID %s" % (dataRef.dataId,))

        for node in self._subgraph.nodes():
            node.input = self.input
            node.execute(dataRef, *args, **kwargs)
            #self.input.mergeItems(node.output, *node.output.getDict().keys())

        #self.output = self.input


    def gconf(self, rootN=''):
        self.list_config = []
        rootN += self.name+'.'
        for node in self._subgraph.nodes():
            if node.task_kind == 'SuperTask':
                for key,val in node.config.iteritems():
                        self.list_config.append(rootN+node.name+'.config.'+key+' = '+str(val))
            else:
                temp_lines = node.gconf(rootN=rootN)
                for line in temp_lines:
                    self.list_config.append(line)
        return self.list_config

