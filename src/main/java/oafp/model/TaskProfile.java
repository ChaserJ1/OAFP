package oafp.model;

/**
 * 包含每个任务的采样率、输入流速、处理时延、checkpoint等信息
 */
public class TaskProfile {
    public String id;
    public double ri; // 当前采样率
    public double Di; // 理想备份量
    public double Li; // 延迟
    public double Pi; // checkpoint 周期
    public double lambda_in; // 输入率
    public double c; // 单位数据checkpoint延迟

    public TaskProfile(String id, double lambda_in, double Pi, double c, double ri) {
        this.id = id;
        this.lambda_in = lambda_in;
        this.Pi = Pi;
        this.c = c;
        this.ri = ri;
        this.Di = Pi * lambda_in;
        this.Li = ri * Di * c;
    }

    public void updateRi(double newRi) {
        this.ri = newRi;
        this.Li = newRi * Di * c;
    }
}
