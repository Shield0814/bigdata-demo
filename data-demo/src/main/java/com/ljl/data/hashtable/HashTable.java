package com.ljl.data.hashtable;

public class HashTable {

    private EmpLinkedList[] empLinkedLists;

    private int capacity;


    public HashTable(int capacity) {
        this.capacity = capacity;
        this.empLinkedLists = new EmpLinkedList[capacity];
        for (int i = 0; i < empLinkedLists.length; i++) {
            empLinkedLists[i] = new EmpLinkedList();
        }
    }

    /**
     * 向散列表中插入数据
     *
     * @param emp
     */
    public void addEmp(Emp emp) {
        int hash = emp.getId().hashCode();
        int index = Math.abs(hash) % capacity;
        empLinkedLists[index].add(emp);
    }

    /**
     * 打印散列表中的数据
     */
    public void list() {
        for (int i = 0; i < empLinkedLists.length; i++) {
            if (!empLinkedLists[i].isEmpty()) {
                empLinkedLists[i].list();
            }
        }
    }

    /**
     * 根据id在散列表中查找数据
     *
     * @param id
     * @return
     */
    public Emp findById(String id) {
        int index = Math.abs(id.hashCode()) % capacity;
        if (!empLinkedLists[index].isEmpty()) {
            return empLinkedLists[index].getById(id);
        }
        return null;
    }

    class EmpLinkedList {

        //头节点，不存任何数据
        private Emp head = new Emp("head", "head", null);

        /**
         * 向链表中添加元素，即添加员工
         *
         * @param emp
         */
        public void add(Emp emp) {

            if (head.getNext() == null) {
                //如果链表中没有数据，则直接添加
                head.setNext(emp);
                return;
            } else {
                //如果链表中有数据，则在链表最后添加该元素
                Emp currentEmp = head.getNext();
                while (currentEmp != null) {
                    currentEmp = currentEmp.getNext();
                }
                currentEmp.setNext(emp);
            }
        }

        /**
         * 打印链表中的元素
         */
        public void list() {
            if (head.getNext() == null) {
                System.out.println("链表为空");
            } else {
                Emp currentEmp = head.getNext();
                do {
                    System.out.println(currentEmp);
                    currentEmp = currentEmp.getNext();
                } while (currentEmp != null);
            }
        }

        /**
         * 通过id查找链表元素
         *
         * @param id
         * @return
         */
        public Emp getById(String id) {

            if (!isEmpty()) {
                Emp currentEmp = head.getNext();
                do {
                    if (currentEmp.getId().equals(id)) {
                        return currentEmp;
                    } else {
                        currentEmp = currentEmp.getNext();
                    }
                } while (currentEmp != null);
            }
            return null;
        }

        /**
         * 链表是否为空
         *
         * @return
         */
        public boolean isEmpty() {
            return this.head.getNext() == null;
        }

    }


    class Emp {

        private String id;

        private String name;

        private Emp next;

        public Emp() {
        }

        public Emp(String id, String name, Emp next) {
            this.id = id;
            this.name = name;
            this.next = next;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Emp getNext() {
            return next;
        }

        public void setNext(Emp next) {
            this.next = next;
        }

        @Override
        public String toString() {
            return "Emp{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    '}';
        }

    }

}


