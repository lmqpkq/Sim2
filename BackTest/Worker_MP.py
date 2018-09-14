import time
import logging
import datetime as DT
import multiprocessing as MP

class Worker_MP_Imp(object) :
    ''' parallel worker with multiprocessing
    '''

    def __init__(self, working_func, alltask_list, \
                 max_proc_num = 0, return_order = True, sleep_second = 0.1):
        ''' work on all tasks in a parallel mode, using multiprocessing
        :param working_func: function of (task_item) => (task_uid, task_out)
        :param alltask_list: list of all task
        :param max_proc_num: max number of sub-processes
        :param return_order: whether to return data with order or out of order
        :param sleep_second: sleep second, used to wait for further operation
        :return: No
        '''

        if len(alltask_list) == 0 :
            raise Exception('No task to work!')

        # const
        self.__working_func = working_func
        self.__alltask_list = alltask_list
        self.__max_proc_num = min(max_proc_num, len(alltask_list)) \
            if max_proc_num > 0 else MP.cpu_count()
        self.__return_order = return_order
        self.__sleep_second = sleep_second

        # variables
        self.__subproc_list = [] # list of all sub-processes
        self.__allretn_list = []
        self.__allretn_dict = {}
        self.__task_queue_I = MP.Queue() # buffer to synchronize input tasks
        self.__task_queue_O = MP.Queue() # buffer to synchronize output tasks

        return

    def work(self, ):
        ''' do all works '''

        self._prepare()
        self._start()
        self._work()
        self._finish()

        return self.__allretn_list

    def _prepare(self, ):
        ''' prepare all sub-processes and tasks '''

        logging.debug('Prepare Begins @ %s' % (DT.datetime.now()))

        # prepare all tasks by putting them into queue_I
        for task_item in self.__alltask_list :
            self.__task_queue_I.put(task_item)

        # prepare all sub-processes
        for iProc in range(self.__max_proc_num) :
            worker_now = MP.Process(target=self.__working_func, \
                args=(iProc, self.__task_queue_I, self.__task_queue_O))
            self.__subproc_list.append(worker_now)

            logging.debug( \
                'Init Proc No.%s: Pid %s, Is_Alive %s, ExitCode %s' % \
                (iProc, worker_now.pid, worker_now.is_alive(), \
                 worker_now.exitcode))

        logging.debug( \
            '%s Sub-Processes, %s Task are all readay' % \
            (self.__max_proc_num, len(self.__alltask_list)))

        logging.debug('Prepare Ends   @ %s' % DT.datetime.now())

        return

    def _start(self, ):
        ''' start all sub-processes '''

        logging.debug('Start Begins @ %s' % (DT.datetime.now()))

        for iProc in range(self.__max_proc_num) :
            worker_now = self.__subproc_list[iProc]
            worker_now.start()

            logging.debug( \
                'Start Proc No.%s: Pid %s, Is_Alive %s, ExitCode %s' % \
                (iProc, worker_now.pid, worker_now.is_alive(), \
                 worker_now.exitcode))

        logging.debug('Start Ends   @ %s' % (DT.datetime.now()))

        return

    def _work(self, ):
        ''' work on all jobs '''

        # wait for all sub-processes to do their jobs

        logging.debug('Wait Begins @ %s' % (DT.datetime.now()))

        # when each sub-process generate an output, all tasks are done
        allretn_data = self.__allretn_dict \
            if self.__return_order else self.__allretn_list
        while len(allretn_data) < len(self.__alltask_list) :
            task_uid, task_out = self.__task_queue_O.get()
            self._add_retn_item(task_uid, task_out)

            logging.debug( \
                'Task get %s, total %s' % \
                (len(allretn_data), len(self.__alltask_list)))

        # convert output data from dict to list with order
        if self.__return_order :
            for retn_item in sorted(self.__allretn_dict.iteritems()) :
                task_uid, task_out = retn_item
                if task_uid is None : # None, [ task_out1, task_out2, ... ]
                    for task_out_item in task_out :
                        self.__allretn_list.append((task_uid, task_out_item))
                else:
                    self.__allretn_list.append(retn_item)

        logging.debug('Wait Ends   @ %s' % (DT.datetime.now()))

        return

    def _finish(self, ):
        ''' clean all sub-processes '''

        logging.debug('Finish Begins @ %s' % (DT.datetime.now()))

        for iProc in range(self.__max_proc_num) :
            worker_now = self.__subproc_list[iProc]
            worker_now.terminate()
            worker_now.join()
            if self.__sleep_second > 0 : # wait for sub-process to exit
                time.sleep(self.__sleep_second)

            logging.debug( \
                'Finish Proc No.%s: Pid %s, Is_Alive %s, ExitCode %s' % \
                (iProc, worker_now.pid, worker_now.is_alive(), \
                 worker_now.exitcode))

        logging.debug('Finish Ends   @ %s' % (DT.datetime.now()))

        return

    def _add_retn_item(self, task_uid, task_out):
        ''' add a return item to all records
        :param task_uid: unique Id of task item
        :param task_out: output task
        :return: No
        '''

        if self.__return_order : # dict
            if task_uid is None :
                # for special data of no valid Id, use 'ErrorXXX'
                error_id = 0
                while True : # avoid the same error Id
                    task_nid = 'Error%06d' % error_id
                    if task_nid in self.__allretn_dict :
                        error_id += 1
                    else:
                        break
                self.__allretn_dict[task_nid] = task_out

            elif task_uid in self.__allretn_dict :
                err_msg = 'Task Id [%s] dulplicates!' % task_uid
                raise Exception(err_msg)

            else:
                self.__allretn_dict[task_uid] = task_out

        else: # list
            task_item = (task_uid, task_out)
            self.__allretn_list.append(task_item)

        return

if __name__ == '__main__':
    pass