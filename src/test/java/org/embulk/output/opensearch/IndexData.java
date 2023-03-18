package org.embulk.output.opensearch;

public class IndexData
{
    private long id;
    private Long account;
    private String time;
    private String purchase;
    private boolean flg;
    private double score;
    private String comment;

    public IndexData(long id, Long account, String time, String purchase, boolean flg, double score, String comment)
    {
        this.id = id;
        this.account = account;
        this.time = time;
        this.purchase = purchase;
        this.flg = flg;
        this.score = score;
        this.comment = comment;
    }

    public IndexData()
    {
    }

    public long getId()
    {
        return id;
    }

    public void setId(long id)
    {
        this.id = id;
    }

    public Long getAccount()
    {
        return account;
    }

    public void setAccount(Long account)
    {
        this.account = account;
    }

    public String getTime()
    {
        return time;
    }

    public void setTime(String time)
    {
        this.time = time;
    }

    public String getPurchase()
    {
        return purchase;
    }

    public void setPurchase(String purchase)
    {
        this.purchase = purchase;
    }

    public boolean getFlg()
    {
        return flg;
    }

    public void setFlg(boolean flg)
    {
        this.flg = flg;
    }

    public double getScore()
    {
        return score;
    }

    public void setScore(double score)
    {
        this.score = score;
    }

    public String getComment()
    {
        return comment;
    }

    public void setComment(String comment)
    {
        this.comment = comment;
    }
}
