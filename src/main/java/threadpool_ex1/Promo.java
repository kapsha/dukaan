package threadpool_ex1;

public class Promo {
	private final long id;
    private final String desc;

    public Promo(long id, String content) {
        this.id = id;
        this.desc = content;
    }

    public long getId() {
        return id;
    }

    public String getContent() {
        return desc;
    }
}
