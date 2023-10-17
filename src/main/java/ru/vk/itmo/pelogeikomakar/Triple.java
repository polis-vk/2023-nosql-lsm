package ru.vk.itmo.pelogeikomakar;

public class Triple<A, B, C> {
    private A first;
    private B second;
    private C third;

    public Triple(A a, B b, C c) {
        first = a;
        second = b;
        third = c;
    }

    public A getFirst() {
        return first;
    }

    public void setFirst(A first) {
        this.first = first;
    }

    public B getSecond() {
        return second;
    }

    public void setSecond(B second) {
        this.second = second;
    }

    public C getThird() {
        return third;
    }

    public void setThird(C third) {
        this.third = third;
    }
}
