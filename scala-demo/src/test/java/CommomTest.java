import java.util.ArrayList;
import java.util.List;

public class CommomTest {

    public static void main(String[] args) {
        List<Super> list = new ArrayList<>();
        list.add(new Super());
        list.add(new Sub());

        list.forEach(x -> x.show());

        Super s1 = new Sub();
        s1.show();

        Super s2 = (Super) new Sub();
        s2.show();

        Sub sub = (Sub) new Super(); //error
        sub.show();

    }
}

class Super {
    public void show() {
        System.out.println("super");
    }
}

class Sub extends Super {
    public void show() {
        System.out.println("Sub");
    }
}