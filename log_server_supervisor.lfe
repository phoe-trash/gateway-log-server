(defmodule log_server_supervisor
  (export (start 0) (start 1) (stop 0) (stop 1)
          (supervisor_initialize 2)))

;;;; UTILS

(defmacro equal-case
  ((list* expr clauses)
   `(case ,expr
      ,@(cl:mapcar (lambda (x) `(_ (when (=:= ,expr ,(car x))) ,@(cdr x)))
                   clauses))))

(eval-when-compile
  (defun atomcat (atom1 atom2)
    (list_to_atom (++ (atom_to_list atom1) (atom_to_list atom2)))))

(defun demonitor_shutdown (pid monref)
  (demonitor monref)
  (exit pid 'shutdown))

(defun curry (f)
  (let ((`#(arity ,arity) (erlang:fun_info f 'arity)))
    (curry f () arity)))

(defun curry (f arg)
  (funcall (curry f) arg))

(defun curry
  ((f args 0) (apply f (lists:reverse args)))
  ((f args arity) (lambda (x) (curry f (cons x args) (- arity 1)))))

;;;; START

(defun start ()
  (start (* 24 60 60)))

(defun start (log_rotate_time)
  (spawn 'log_server/supervisor 'supervisor_initialize (list log_rotate_time)))

(defun spawn_wait (module function arguments)
  (spawn_wait module function arguments 5000))

(defun spawn_wait (module function arguments delay)
  (let ((pid (spawn module function arguments)))
    (receive (#(ok init) pid)
             (after delay
               (exit pid 'kill)
               #(error failed-to-start)))))

;;;; STOP

(defun stop ()
  (stop 30))

(defun stop (timeout)
  (let* ((pid (whereis 'log_server/supervisor))
         (monref (monitor 'process pid)))
    (! pid #(stop))
    (receive (`#(DOWN ,monref process ,pid normal) 'ok)
             (`#(DOWN ,monref process ,pid ,error)
              `#(error #(abnormal-halt ,error)))
             (after timeout `#(error #(failed_to-stop ,pid))))))

;;;; SUPERVISOR_INITIALIZE

(defun supervisor_initialize (parent log_rotate_time)
  (register 'log_server/supervisor (self))
  (let* ((supervisor (self))
         (`#(ok ,store) (start_store supervisor))
         (`#(ok ,cache) (start_cache supervisor store))
         (`#(ok ,timer) (start_timer supervisor cache log_rotate_time))
         (`#(ok ,api) (start_api supervisor store cache timer))
         (atoms '(store cache timer api))
         (processes (list store cache timer api))
         (monitors (cl:mapcar (curry #'monitor/2 'process) processes))
         (loopdata (list* parent log_rotate_time
                          (lists:zip3 atoms processes monitors))))
    (supervisor_loop loopdata)))

(defun start_store (supervisor)
  (spawn_wait 'log_server/store 'store_initialize (list supervisor) 30))

(defun start_cache (supervisor store)
  (spawn_wait 'log_server/cache 'cache_initialize (list supervisor store) 10))

(defun start_timer (supervisor cache log_rotate_time)
  (spawn_wait 'log_server/timer 'timer_initialize
              (list supervisor cache log_rotate_time)))

(defun start_api (supervisor store cache timer)
  (spawn_wait 'log_server/api 'api_initialize
              (list supervisor store cache timer)))

;;;; SUPERVISOR_LOOP

(defun supervisor_loop (loopdata)
  (receive
    (#(stop)
     (supervisor_stop loopdata))
    (`#(DOWN ,_ process ,pid ,reason)
     (supervisor_loop (supervisor_down loopdata pid reason)))
    (msg
     (let ((parent (car loopdata)))
       (! parent `#(warn #(invalid_message ,msg))))
     (supervisor_loop loopdata))))

;;;; SUPERVISOR_DOWN AND MACROS

(eval-when-compile
  (defun %down_state ()
    '`(,parent ,log_rotate_time
               #(store ,store ,store_mon) #(cache ,cache ,cache_mon)
               #(timer ,timer ,timer_mon) #(api ,api ,api_mon)))

  (defun %down_args (symbol)
    (cons 'supervisor (case symbol
                        ('store '()) ('cache '(store))
                        ('timer '(cache log_rotate_time))
                        ('api '(store cache timer)))))

  (defun %down_list (symbol)
    (let ((symbols '(store cache timer api)))
      (lists:dropwhile (lambda (x) (not (=:= x symbol))) symbols)))

  (defun %down_demonitors (symbol)
    (let ((symbols (cdr (%down_list symbol))))
      (cl:mapcar (lambda (x) `(demonitor_shutdown ,x ,(atomcat x '_mon)))
                 symbols)))

  (defun %%down_let* (symbol)
    `((,symbol (,(atomcat 'start_ symbol) ,@(%down_args symbol)))
      (,(atomcat symbol '_mon) (monitor 'process ,symbol))))

  (defun %down_let* (symbol)
    (let* ((symbols (%down_list symbol))
           (clauses (cl:mapcar #'%%down_let*/1 symbols)))
      (lists:append clauses))))

(defmacro %down_macro (symbol)
  `(progn (! parent (tuple 'CHILD_DOWN ,symbol reason))
          ,@(%down_demonitors symbol)
          (let* ,(%down_let* symbol)
            ,(%down_state))))

(defun supervisor_down (loopdata pid reason)
  (let ((`(,parent
           ,log_rotate_time
           #(store ,store ,store_mon) #(cache ,cache ,cache_mon)
           #(timer ,timer ,timer_mon) #(api ,api ,api_mon))
         loopdata)
        (supervisor (self)))
    (equal-case pid
                (api (%down_macro api))
                (timer (%down_macro timer))
                (cache (%down_macro cache))
                (store (%down_macro store)))))

(defun supervisor_stop (loopdata)
  (let ((`(,parent
           ,log_rotate_time
           #(store ,store ,store_mon) #(cache ,cache ,cache_mon)
           #(timer ,timer ,timer_mon) #(api ,api ,api_mon))
         loopdata)
        (supervisor (self)))
    (cl:mapcar (match-lambda ((elt mon) (demonitor_shutdown elt mon)))
               `((,api ,api_mon)
                 (,timer ,timer_mon)
                 (,cache ,cache_mon)
                 (,store ,store_mon)))))
