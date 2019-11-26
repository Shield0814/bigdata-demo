package com.ljl.data.tree;

class HeroNode {

    private String no;
    private String name;

    private HeroNode left;

    private HeroNode right;


    /**
     * 前序遍历
     */
    public void preOrderTraversal() {
        System.out.println(this);
        if (this.left != null) {
            this.left.preOrderTraversal();
        }
        if (this.right != null) {
            this.right.preOrderTraversal();
        }
    }

    /**
     * 通过前序遍历查找值
     *
     * @param id
     * @return
     */
    public HeroNode getWithPreOrderTraversal(String id) {
        System.out.println(">" + this);
        HeroNode res = null;
        if (this.no.equals(id)) {
            return this;
        }
        if (this.left != null) {
            res = this.left.getWithPreOrderTraversal(id);
        }
        if (res != null) {
            return res;
        }
        if (this.right != null) {
            res = this.right.getWithPreOrderTraversal(id);
        }
        if (res != null) {
            return res;
        }
        return null;
    }


    public void deleteNode(String id) {
        if (this.left != null && this.left.no.equals(id)) {
            this.left = null;
            return;
        }
        if (this.right != null && this.right.no.equals(id)) {
            this.right = null;
            return;
        }
        if (this.left != null) {
            this.left.deleteNode(id);
        }

        if (this.right != null) {
            this.right.deleteNode(id);
        }
    }

    /**
     * 中序遍历
     */
    public void inOrderTraversal() {
        if (this.left != null) {
            this.left.inOrderTraversal();
        }
        System.out.println(this);
        if (this.right != null) {
            this.right.inOrderTraversal();
        }
    }

    /**
     * 通过中序遍历查找值
     *
     * @param id
     * @return
     */
    public HeroNode getWithInOrderTraversal(String id) {
        System.out.println(">" + this);
        HeroNode res = null;
        if (this.left != null) {
            res = this.left.getWithInOrderTraversal(id);
        }
        if (res != null) {
            return res;
        }
        if (this.no.equals(id)) {
            return this;
        }
        if (this.right != null) {
            res = this.right.getWithInOrderTraversal(id);
        }
        if (res != null) {
            return res;
        }
        return null;
    }

    /**
     * 后序遍历
     */
    public void postOrderTraversal() {
        if (this.left != null) {
            this.left.postOrderTraversal();
        }
        if (this.right != null) {
            this.right.postOrderTraversal();
        }
        System.out.println(this);
    }

    /**
     * 通过后序遍历查找值
     *
     * @param id
     * @return
     */
    public HeroNode getWithPostOrderTraversal(String id) {
        System.out.println(">" + this);
        HeroNode res = null;
        if (this.left != null) {
            res = this.left.getWithPostOrderTraversal(id);
        }
        if (res != null) {
            return res;
        }
        if (this.right != null) {
            res = this.right.getWithPostOrderTraversal(id);
        }
        if (res != null) {
            return res;
        }
        if (this.no.equals(id)) {
            return this;
        }
        return null;
    }

    public HeroNode() {
        super();
    }

    public HeroNode(String no, String name) {
        super();
        this.no = no;
        this.name = name;
    }

    public String getNo() {
        return no;
    }

    public void setNo(String no) {
        this.no = no;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public HeroNode getLeft() {
        return left;
    }

    public void setLeft(HeroNode left) {
        this.left = left;
    }

    public HeroNode getRight() {
        return right;
    }

    public void setRight(HeroNode right) {
        this.right = right;
    }

    @Override
    public String toString() {
        return "HeroNode{" +
                "no='" + no + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}