/*
 * Visualizator
 */
package xklusac.extensions;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import javax.imageio.ImageIO;
import javax.swing.*;
/**
 * Class Visualizator<p>
 * It is used to create a JPanel where graphs are drawn. What is drawn is driven by the paintComponent(Graphics g) method.<p>
 * If you want to change the graphical output, rewrite this method.
 * @author Dalibor Klusacek
 */
public class Visualizator extends JPanel implements Runnable {
    
    BufferedImage image;
    int scale = 1;
    Thread thread;
    boolean animating = false;
    boolean increasing = true;
    public final static Color brown = new Color(116, 74, 8);
    public final static Color green2 = new Color(123, 255, 5);
    public final static Color red2 = new Color(136, 24, 2);
    public final static Color blue2 = new Color(14, 219, 232);
    public final static Color yellow2 = new Color(196, 176, 12);
    public final static Color orange2 = new Color(203, 76, 0);
    public final static Color gray2 = new Color(120, 152, 118);
    public final static Color brown2 = new Color(148, 114, 70);
    public final static Color blue3 = new Color(1, 15, 16);
    Color colors[] = {Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2,
    Color.red, Color.BLUE, Color.green, Color.yellow, Color.gray,
    Color.ORANGE, Color.cyan, Color.MAGENTA, Color.pink, Color.darkGray, Color.black,
    brown, green2, red2, blue2, yellow2, orange2, gray2, blue3, brown2
            
    };
    Random r = new Random(1);
    long time = 0;
    boolean usage = false;
    boolean day_usage = false;
    boolean full_usage = false;
    boolean full_wait = false;
    boolean full_run = false;
    LinkedList<Integer> days = new LinkedList();
    LinkedList<Double> util = new LinkedList();
    LinkedList<Double> cl_util = new LinkedList();
    LinkedList<Double> cl_status = new LinkedList();
    LinkedList<Double> cl_util_h = new LinkedList();
    LinkedList<Double> day_util = new LinkedList();
    LinkedList<String> cl_names = new LinkedList();
    LinkedList<Integer> running = new LinkedList();
    LinkedList<Integer> waiting = new LinkedList();
    LinkedList<Integer> requested = new LinkedList();
    LinkedList<Integer> used = new LinkedList();
    LinkedList<Integer> hours = new LinkedList();
    LinkedList<Integer> availCPUs = new LinkedList();
    boolean cl_usage = false;
    boolean cl_usage_col = false;
    boolean dcl_usage = false;
    boolean rw = false;
    boolean ru = false;
    int cl_count = 0;
    int totCPUs = 0;
    boolean cl_status_col = false;
    
    /**
     * Constructor. Creates one JPanel inside some JFrame. JPanel will be used to paint on graphs.
     *
     */
    public Visualizator() {
    }
    
    /**
     * Rewrite this method to obtain new graphical output. Get inspired by the present code.
     *
     */
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);
        Graphics2D g2 = (Graphics2D) g;
        g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
                RenderingHints.VALUE_INTERPOLATION_BICUBIC);
        
        int w = getWidth();
        int h = getHeight();
        g2.setColor(Color.black);
        
        if (usage) {
            //draw y axis
            g2.drawLine(50, 20, 50, h - 50);
            int y_count = 100;
            int y_step = Math.max(1, (Math.round(((h - 70)) / y_count)));
            int y = 1;
            g2.drawString("0", 25, h - 45);
            g2.drawLine(50, h - 50, 50, h - 44);
            while ((h - 50) - (y * y_step) > 20) {
                if (y > y_count) {
                    break;
                }
                int ys = (h - 50) - (y * y_step);
                if (y % 10 == 0) {
                    g2.drawLine(50, ys, 47, ys);
                }
                if (y % 10 == 0) {
                    g2.drawLine(50, ys, 44, ys);
                    int factor = 3;
                    if (y < 10) {
                        factor = 3;
                    } else if (y < 100) {
                        factor = 5;
                    } else if (y >= 100) {
                        factor = 8;
                    }
                    g2.drawString("" + y, 25 - factor, ys + 4);
                }
                y++;
            }
            
            
            
            // draw x axis
            g2.drawLine(50, h - 50, w - 10, h - 50);
            int x_count = days.size();
            int x_step = Math.max(1, (Math.round(((w - 60)) / x_count)));
            int x = 1;
            //System.out.println("x-step = "+x_step);
            g2.drawString("0", 47, h - 25);
            g2.drawLine(50, h - 50, 50, h - 44);
            int x_point = 0;
            int x_num = 0;
            if (x_step >= 50) {
                x_point = 1;
                x_num = 1;
            } else if (x_step < 50 && x_step >= 20) {
                x_point = 1;
                x_num = 5;
            } else {
                x_point = 5;
                x_num = 10;
            }
            while ((x * x_step) + 50 < (w - 10)) {
                int xs = (x * x_step) + 50;
                if (x % x_point == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 47);
                }
                if (x % x_num == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 44);
                    int factor = 3;
                    if (x < 10) {
                        factor = 3;
                    } else if (x < 100) {
                        factor = 5;
                    } else if (x >= 100) {
                        factor = 8;
                    }
                    g2.drawString("" + x, xs - factor, h - 25);
                }
                x++;
            }
            
            
            g2.drawString("Average machine usage per day [%]", w / 2 - 70, 15);
            g2.drawString("days", w / 2 - 10, h - 5);
            int xs = 50;
            int ys = h - 50;
            int last_x = xs;
            int last_y = ys;
            g2.setColor(Color.red);
            for (int i = 1; i <= days.size(); i++) {
                //int day = days.get(i - 1);
                double ut = util.get(i - 1);
                Long dist = Math.round(y_step * ut);
                int length = Integer.valueOf(dist.intValue());
                g2.drawLine(last_x, last_y, xs + i * x_step, ys - length);
                if (!full_usage) {
                    g2.drawLine(xs + i * x_step, ys, xs + i * x_step, ys - length);
                }
                last_x = xs + i * x_step;
                last_y = ys - length;
            }
            
            this.usage = true;
        }
        if (cl_usage) {
            //draw y axis
            g2.drawLine(50, 20, 50, h - 50);
            int y_count = 100;
            int y_step = Math.max(1, (Math.round(((h - 70)) / y_count)));
            int y = 1;
            g2.drawString("0", 25, h - 45);
            g2.drawLine(50, h - 50, 50, h - 44);
            while ((h - 50) - (y * y_step) > 20) {
                if (y > y_count) {
                    break;
                }
                int ys = (h - 50) - (y * y_step);
                if (y % 10 == 0) {
                    g2.drawLine(50, ys, 47, ys);
                }
                if (y % 10 == 0) {
                    g2.drawLine(50, ys, 44, ys);
                    int factor = 3;
                    if (y < 10) {
                        factor = 3;
                    } else if (y < 100) {
                        factor = 5;
                    } else if (y >= 100) {
                        factor = 8;
                    }
                    g2.drawString("" + y, 25 - factor, ys + 4);
                }
                y++;
            }
            
            
            
            // draw x axis
            g2.drawLine(50, h - 50, w - 220, h - 50);
            int x_count = days.size();
            int x_step = Math.max(1, (Math.round(((w - 220)) / x_count)));
            int x = 1;
            g2.drawString("0", 47, h - 25);
            g2.drawLine(50, h - 50, 50, h - 44);
            int x_point = 0;
            int x_num = 0;
            if (x_step >= 50) {
                x_point = 1;
                x_num = 1;
            } else if (x_step < 50 && x_step >= 20) {
                x_point = 1;
                x_num = 5;
            } else {
                x_point = 5;
                x_num = 10;
            }
            while ((x * x_step) + 50 <= (w - 220)) {
                int xs = (x * x_step) + 50;
                if (x % x_point == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 47);
                }
                if (x % x_num == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 44);
                    int factor = 3;
                    if (x < 10) {
                        factor = 3;
                    } else if (x < 100) {
                        factor = 5;
                    } else if (x >= 100) {
                        factor = 8;
                    }
                    g2.drawString("" + x, xs - factor, h - 25);
                }
                x++;
            }
            
            
            g2.drawString("Cluster usage per day [%]", w / 2 - 70, 15);
            g2.drawString("days", w / 2 - 10, h - 5);
            int cl_pointer = 0;
            int xs[] = new int[cl_count];
            int ys[] = new int[cl_count];
            int last_x[] = new int[cl_count];
            int last_y[] = new int[cl_count];
            for (int c = 0; c < cl_count; c++) {
                xs[c] = 50;
                ys[c] = h - 50;
                last_x[c] = 50;
                last_y[c] = h - 50;
            }
            for (int i = 1; i <= days.size(); i++) {
                for (int c = 0; c < cl_count; c++) {
                    g2.setColor(colors[c]);
                    //int day = days.get(i - 1);
                    double ut = Math.max(0.0, cl_util.get(cl_pointer));
                    Long dist = Math.round(y_step * ut);
                    int length = Integer.valueOf(dist.intValue());
                    g2.drawLine(last_x[c], last_y[c], xs[c] + i * x_step, ys[c] - length);
                    if (full_usage) {
                        g2.drawLine(xs[c] + i * x_step, ys[c], xs[c] + i * x_step, ys[c] - length);
                    }
                    last_x[c] = xs[c] + i * x_step;
                    last_y[c] = ys[c] - length;
                    cl_pointer++;
                }
            }
            
            for (int c = 0; c < cl_count; c++) {
                g2.setColor(colors[c]);
                int sub = Math.min(cl_names.get(c).length(), 20);
                String name = cl_names.get(c).substring(0, sub);
                g2.drawString(name + "...", w - 150, 20 + 13 * c);
                //g2.setColor(colors[c]);
                //g2.drawLine(w - 20, 5 + 5 * c, w - 10, 5 + 5 * c);
            }
            
            this.cl_usage = true;
        }
        if (rw) {
            //draw y axis
            int max = 0;
            for (int s = 0; s < waiting.size(); s++) {
                int curr = Math.max(waiting.get(s), running.get(s));
                if (curr > max) {
                    max = curr;
                }
            }
            g2.drawLine(60, 20, 60, h - 50);
            double y_count = max;
            //int y_step = Math.max(1, (Math.round(((h - 70)) / y_count)));
            double y_step = (h - 70) / y_count;
            
            Long when_textl = Math.round((y_count / (h - 70)) * 20);
            int when_text = Integer.valueOf(when_textl.intValue());
            when_text = Math.max(1, when_text);
            //System.out.println("---------------------------------------- y step "+y_step);
            int y = 1;
            g2.drawString("0", 45, h - 45);
            g2.drawLine(60, h - 50, 60, h - 44);
            while ((h - 50) - (y * y_step) > 20) {
                
                Long lys = Math.round((h - 50) - (y * y_step));
                int ys = Integer.valueOf(lys.intValue());
                /*if (y % 10 == 0) {
                g2.drawLine(50, ys, 47, ys);
                }*/
                if (y % when_text == 0) {
                    g2.drawLine(60, ys, 54, ys);
                    int factor = 3;
                    if (y < 10) {
                        factor = 3;
                    } else if (y < 100) {
                        factor = 5;
                    } else if (y >= 100) {
                        factor = 8;
                    }
                    g2.drawString("" + y, 25 - factor, ys + 4);
                }
                y++;
            }
            
            
            
            // draw x axis
            g2.drawLine(60, h - 50, w - 10, h - 50);
            int x_count = days.size();
            int x_step = Math.max(1, (Math.round(((w - 145)) / x_count)));
            int x = 1;
            g2.drawString("0", 57, h - 25);
            g2.drawLine(60, h - 50, 60, h - 44);
            int x_point = 0;
            int x_num = 0;
            if (x_step >= 50) {
                x_point = 1;
                x_num = 1;
            } else if (x_step < 50 && x_step >= 20) {
                x_point = 1;
                x_num = 5;
            } else {
                x_point = 5;
                x_num = 25;
            }
            while ((x * x_step) + 60 < (w - 10)) {
                int xs = (x * x_step) + 50;
                if (x % x_point == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 47);
                }
                if (x % x_num == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 44);
                    int factor = 3;
                    if (x < 10) {
                        factor = 3;
                    } else if (x < 100) {
                        factor = 5;
                    } else if (x >= 100) {
                        factor = 8;
                    }
                    g2.drawString("" + x, xs - factor, h - 25);
                }
                x++;
            }
            
            
            g2.drawString("Number of waiting/running jobs", w / 2 - 70, 15);
            g2.drawString(" days", w / 2 - 10, h - 5);
            
            int xs = 60;
            int ys = h - 50;
            int last_x = xs;
            int last_y = ys;
            g2.setColor(Color.red);
            for (int i = 1; i <= days.size(); i++) {
                
                //int day = days.get(i - 1);
                double ut = waiting.get(i - 1);
                Long dist = Math.round(y_step * ut);
                int length = Integer.valueOf(dist.intValue());
                g2.drawLine(last_x, last_y, xs + i * x_step, ys - length);
                //System.out.println(last_x+" , "+ last_y+" , "+(xs + i * x_step)+" , " +(ys - length));
                if (full_wait) {
                    g2.drawLine(xs + i * x_step, ys, xs + i * x_step, ys - length);
                }
                last_x = xs + i * x_step;
                last_y = ys - length;
            }
            xs = 60;
            ys = h - 50;
            last_x = xs;
            last_y = ys;
            g2.setColor(Color.green);
            for (int i = 1; i <= days.size(); i++) {
                //int day = days.get(i - 1);
                double ut = running.get(i - 1);
                Long dist = Math.round(y_step * ut);
                int length = Integer.valueOf(dist.intValue());
                g2.drawLine(last_x, last_y, xs + i * x_step, ys - length);
                if (full_run) {
                    g2.drawLine(xs + i * x_step, ys, xs + i * x_step, ys - length);
                }
                last_x = xs + i * x_step;
                last_y = ys - length;
            }
            
            
            
            g2.setColor(Color.red);
            g2.drawString("waiting jobs", w - 100, 20 + 13 * 0);
            g2.setColor(Color.green);
            g2.drawString("running jobs", w - 100, 20 + 13 * 1);
            
            
            
            this.rw = true;
        }
        if (ru) {
            //draw y axis
            int max = totCPUs;
            for (int s = 0; s < used.size(); s++) {
                int curr = Math.max(used.get(s), requested.get(s));
                if (curr > max) {
                    max = curr;
                }
            }
            g2.drawLine(50, 20, 50, h - 50);
            double y_count = max;
            //int y_step = Math.max(1, (Math.round(((h - 70)) / y_count)));
            double y_step = (h - 70) / y_count;
            
            Long when_textl = Math.round((y_count / (h - 70)) * 20);
            int when_text = Integer.valueOf(when_textl.intValue());
            //System.out.println("---------------------------------------- y step "+y_step);
            int y = 1;
            g2.drawString("0", 25, h - 45);
            g2.drawLine(50, h - 50, 50, h - 44);
            while ((h - 50) - (y * y_step) > 20) {
                
                Long lys = Math.round((h - 50) - (y * y_step));
                int ys = Integer.valueOf(lys.intValue());
                /*if (y % 10 == 0) {
                g2.drawLine(50, ys, 47, ys);
                }*/
                if (y % when_text == 0) {
                    g2.drawLine(50, ys, 44, ys);
                    int factor = 3;
                    if (y < 10) {
                        factor = 3;
                    } else if (y < 100) {
                        factor = 5;
                    } else if (y >= 100) {
                        factor = 8;
                    }
                    g2.drawString("" + y, 25 - factor, ys + 4);
                }
                y++;
            }
            
            
            
            // draw x axis
            g2.drawLine(50, h - 50, w - 10, h - 50);
            int x_count = days.size();
            int x_step = Math.max(1, (Math.round(((w - 145)) / x_count)));
            int x = 1;
            g2.drawString("0", 47, h - 25);
            g2.drawLine(50, h - 50, 50, h - 44);
            int x_point = 0;
            int x_num = 0;
            if (x_step >= 50) {
                x_point = 1;
                x_num = 1;
            } else if (x_step < 50 && x_step >= 20) {
                x_point = 1;
                x_num = 5;
            } else {
                x_point = 5;
                x_num = 10;
            }
            while ((x * x_step) + 50 < (w - 10)) {
                int xs = (x * x_step) + 50;
                if (x % x_point == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 47);
                }
                if (x % x_num == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 44);
                    int factor = 3;
                    if (x < 10) {
                        factor = 3;
                    } else if (x < 100) {
                        factor = 5;
                    } else if (x >= 100) {
                        factor = 8;
                    }
                    g2.drawString("" + x, xs - factor, h - 25);
                }
                x++;
            }
            
            
            g2.drawString("Number of requested and used CPUS", w / 2 - 70, 15);
            g2.drawString(" days", w / 2 - 10, h - 5);
            
            int xs = 50;
            int ys = h - 50;
            int last_x = xs;
            int last_y = ys;
            g2.setColor(Color.blue);
            for (int i = 1; i <= days.size(); i++) {
                
                //int day = days.get(i - 1);
                double ut = requested.get(i - 1);
                Long dist = Math.round(y_step * ut);
                int length = Integer.valueOf(dist.intValue());
                g2.drawLine(last_x, last_y, xs + i * x_step, ys - length);
                //System.out.println(last_x+" , "+ last_y+" , "+(xs + i * x_step)+" , " +(ys - length));
                if (full_wait) {
                    g2.drawLine(xs + i * x_step, ys, xs + i * x_step, ys - length);
                }
                last_x = xs + i * x_step;
                last_y = ys - length;
            }
            xs = 50;
            ys = h - 50;
            last_x = xs;
            last_y = ys;
            g2.setColor(Color.green);
            for (int i = 1; i <= days.size(); i++) {
                //int day = days.get(i - 1);
                double ut = used.get(i - 1);
                Long dist = Math.round(y_step * ut);
                int length = Integer.valueOf(dist.intValue());
                g2.drawLine(last_x, last_y, xs + i * x_step, ys - length);
                if (full_run) {
                    g2.drawLine(xs + i * x_step, ys, xs + i * x_step, ys - length);
                }
                last_x = xs + i * x_step;
                last_y = ys - length;
            }
            
            
            
            
            g2.setColor(Color.red);
            last_x = 50;
            ys = h - 50;
            for (int i = 1; i <= days.size(); i++) {
                //int day = days.get(i - 1);
                double ut = availCPUs.get(i - 1);
                Long dist = Math.round(y_step * ut);
                int length = Integer.valueOf(dist.intValue());
                g2.drawLine(last_x, ys - length, xs + i * x_step, ys - length);
                last_x = xs + i * x_step;
            }
            
            //Long lys = Math.round((h - 50) - (totCPUs * y_step));
            //ys = Integer.valueOf(lys.intValue());
            //g2.drawLine(50, ys, w - 105, ys);
            
            
            
            g2.setColor(Color.blue);
            g2.drawString("requested CPUs", w - 100, 20 + 13 * 0);
            g2.setColor(Color.green);
            g2.drawString("used CPUs", w - 100, 20 + 13 * 1);
            g2.setColor(Color.red);
            g2.drawString("available CPUs", w - 100, 20 + 13 * 2);
            
            
            
            
            this.ru = true;
        }
        
        if (day_usage) {
            //draw y axis
            g2.drawLine(50, 20, 50, h - 50);
            int y_count = 100;
            int y_step = Math.max(1, (Math.round(((h - 70)) / y_count)));
            int y = 1;
            g2.drawString("0", 25, h - 45);
            g2.drawLine(50, h - 50, 50, h - 44);
            while ((h - 50) - (y * y_step) > 20) {
                if (y > y_count) {
                    break;
                }
                int ys = (h - 50) - (y * y_step);
                if (y % 10 == 0) {
                    g2.drawLine(50, ys, 47, ys);
                }
                if (y % 10 == 0) {
                    g2.drawLine(50, ys, 44, ys);
                    int factor = 3;
                    if (y < 10) {
                        factor = 3;
                    } else if (y < 100) {
                        factor = 5;
                    } else if (y >= 100) {
                        factor = 8;
                    }
                    g2.drawString("" + y, 25 - factor, ys + 4);
                }
                y++;
            }
            
            
            
            // draw x axis
            g2.drawLine(50, h - 50, w - 10, h - 50);
            int x_count = 24;
            int x_step = Math.max(1, (Math.round(((w - 60)) / x_count)));
            int x = 1;
            //System.out.println("x-step = "+x_step);
            g2.drawString("0", 47, h - 25);
            g2.drawLine(50, h - 50, 50, h - 44);
            int x_point = 0;
            int x_num = 0;
            
            if (x_step >= 20) {
                x_point = 1;
                x_num = 1;
            } else {
                x_point = 1;
                x_num = 3;
            }
            while ((x * x_step) + 50 < (w - 10) && x < 25) {
                int xs = (x * x_step) + 50;
                if (x % x_point == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 47);
                }
                if (x % x_num == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 44);
                    int factor = 3;
                    if (x < 10) {
                        factor = 3;
                    } else if (x < 100) {
                        factor = 5;
                    } else if (x >= 100) {
                        factor = 8;
                    }
                    g2.drawString("" + x, xs - factor, h - 25);
                }
                x++;
            }
            time = time / (3600 * 24);
            time++;
            g2.drawString("Average machine usage per hour [%] in day = " + time, w / 2 - 120, 15);
            g2.drawString("hours", w / 2 - 10, h - 5);
            int xs = 50;
            int ys = h - 50;
            int last_x = xs;
            int last_y = ys;
            g2.setColor(Color.red);
            //System.out.println(hours.size()+" hodin, dat = "+day_util.size()+": ");
            for (int i = 1; i <= hours.size(); i++) {
                //System.out.print(hours.get(i-1)+",");
                //int day = days.get(i - 1);
                double ut = day_util.get(i - 1);
                Long dist = Math.round(y_step * ut);
                int length = Integer.valueOf(dist.intValue());
                g2.drawLine(last_x, last_y, xs + i * x_step, ys - length);
                if (!full_usage) {
                    g2.drawLine(xs + i * x_step, ys, xs + i * x_step, ys - length);
                }
                last_x = xs + i * x_step;
                last_y = ys - length;
            }
            
            this.day_usage = true;
        }
        
        if (dcl_usage) {
            //draw y axis
            g2.drawLine(150, 20, 150, h - 50);
            int y_count = cl_count;
            int y_step = Math.max(1, (Math.round(((h - 70)) / y_count)));
            for (int c = 0; c < cl_count; c++) {
                g2.setColor(colors[c]);
                int sub = Math.min(cl_names.get(c).length(), 20);
                String name = cl_names.get(c).substring(0, sub);
                g2.drawString(name + "...", 5, h - 50 - (c) * y_step);
                //g2.setColor(colors[c]);
                //g2.drawLine(w - 20, 5 + 5 * c, w - 10, 5 + 5 * c);
            }
            
            g2.drawLine(150, h - 50, 150, h - 44);
            int y_point = 0;
            int y_num = 0;
            
            if (y_step >= 20) {
                y_point = 1;
                y_num = 1;
            } else {
                y_point = 2;
                y_num = 5;
            }
            
            g2.setColor(Color.black);
            // draw x axis
            g2.drawLine(150, h - 50, w - 10, h - 50);
            int x_count = 100;
            int x_step = Math.max(1, (Math.round(((w - 220)) / x_count)));
            int x = 1;
            g2.drawString("0", 147, h - 25);
            g2.drawLine(150, h - 50, 150, h - 44);
            int x_point = 0;
            int x_num = 0;
            if (x_step >= 30) {
                x_point = 1;
                x_num = 1;
            } else if (x_step < 30 && x_step >= 5) {
                x_point = 1;
                x_num = 5;
            } else {
                x_point = 5;
                x_num = 25;
            }
            while ((x * x_step) + 150 < (w - 10) && x < 101) {
                int xs = (x * x_step) + 150;
                if (x % x_point == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 47);
                }
                if (x % x_num == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 44);
                    int factor = 3;
                    if (x < 10) {
                        factor = 3;
                    } else if (x < 100) {
                        factor = 5;
                    } else if (x >= 100) {
                        factor = 8;
                    }
                    g2.drawString("" + x, xs - factor, h - 25);
                }
                x++;
            }
            time = time / (3600 * 24);
            time++;
            g2.drawString("Cluster usage per hour[%] in day = " + time, w / 2 - 70, 15);
            g2.drawString("usage [%]", w / 2 - 10, h - 5);
            
            
            //for (int c = cl_count-1; c >= 0; c--) {
            for (int c = 0; c < cl_count; c++) {
                g2.setColor(colors[c]);
                //int day = days.get(i - 1);
                double ut = cl_util_h.get(c);
                if (ut < 0.0) {
                    Color cl = new Color(68, 68, 68);
                    g2.setColor(cl);
                    ut = 0.0;
                    Long dist = Math.round(x_step * ut);
                    int length = Integer.valueOf(dist.intValue());
                    g2.drawString("resource down...", 152 + length, h - 51 - (c) * y_step);
                } else {
                    Long dist = Math.round(x_step * ut);
                    int length = Integer.valueOf(dist.intValue());
                    g2.drawLine(151, h - 50 - (c) * y_step, 151 + length, h - 50 - (c) * y_step);
                    int max = Math.max(1, y_step - 1);
                    for (int width = 0; width < max; width++) {
                        g2.drawLine(151, h - 50 - (c) * y_step - width, 151 + length, h - 50 - (c) * y_step - width);
                    }
                    //g2.drawLine(150, h-51-(c+1)*y_step, 150+length, h-51-(c+1)*y_step);
                    //g2.drawLine(150, h-52-(c+1)*y_step, 150+length, h-52-(c+1)*y_step);
                    g2.drawString(ut + "%", 152 + length, h - 51 - (c) * y_step);
                }
            /*if (full_usage) {
            g2.drawLine(xs[c] + i * x_step, ys[c], xs[c] + i * x_step, ys[c] - length);
            }*/
                
            }
            
            
            
            
            this.dcl_usage = true;
        }
        if (cl_usage_col) {
            //draw y axis
            g2.drawLine(150, 20, 150, h - 50);
            int y_count = cl_count;
            int y_step = Math.max(1, (Math.round(((h - 70)) / y_count)));
            for (int c = 0; c < cl_count; c++) {
                //g2.setColor(colors[c]);
                int sub = Math.min(cl_names.get(c).length(), 20);
                String name = cl_names.get(c).substring(0, sub);
                g2.drawString(name + "...", 5, h - 50 - (c) * y_step);
                //g2.setColor(colors[c]);
                //g2.drawLine(w - 20, 5 + 5 * c, w - 10, 5 + 5 * c);
            }
            
            g2.drawLine(150, h - 50, 150, h - 44);
            int y_point = 0;
            int y_num = 0;
            
            if (y_step >= 20) {
                y_point = 1;
                y_num = 1;
            } else {
                y_point = 2;
                y_num = 5;
            }
            
            g2.setColor(Color.black);
            
            
            
            // draw x axis
            g2.drawLine(150, h - 50, w - 50, h - 50);
            int x_count = days.size();
            int x_step = Math.max(1, (Math.round(((w - 200)) / x_count)));
            int x = 1;
            g2.drawString("0", 147, h - 25);
            g2.drawLine(150, h - 50, 150, h - 44);
            int x_point = 0;
            int x_num = 0;
            if (x_step >= 30) {
                x_point = 1;
                x_num = 1;
            } else if (x_step < 30 && x_step >= 10) {
                x_point = 1;
                x_num = 5;
            } else if (x_step < 10 && x_step >= 5) {
                x_point = 5;
                x_num = 10;
            } else {
                x_point = 5;
                x_num = 25;
            }
            while ((x * x_step) + 190 <= (w - 10)) {
                int xs = (x * x_step) + 150;
                if (x % x_point == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 47);
                }
                if (x % x_num == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 44);
                    int factor = 3;
                    if (x < 10) {
                        factor = 3;
                    } else if (x < 100) {
                        factor = 5;
                    } else if (x >= 100) {
                        factor = 8;
                    }
                    g2.drawString("" + x, xs - factor, h - 25);
                }
                x++;
            }
            
            //g2.drawString(x_step+" = step: "+x +"%"+ x_num+" = "+x % x_num, w / 2 - 70, 15);
            g2.drawString("Cluster usage per day [%]", w / 2 - 70, 15);
            g2.drawString("days", w / 2 - 10, h - 5);
            int cl_pointer = 0;
            int xs[] = new int[cl_count];
            int last_x[] = new int[cl_count];
            for (int c = 0; c < cl_count; c++) {
                xs[c] = 151;
                last_x[c] = 151;
            }
            for (int i = 1; i <= days.size(); i++) {
                for (int c = 0; c < cl_count; c++) {
                    double ut = cl_util.get(cl_pointer);
                    if (ut < 0.0) {
                        Color cl = new Color(68, 68, 68);
                        g2.setColor(cl);
                    } else {
                        ut = ut / 100.0;
                        int rd = 0;
                        int gr = 0;
                        int b = 0;
                        Long dist = 0l;
                        if (ut < 0.5) {
                            dist = Math.round(255 * ut * 2);
                            rd = Integer.valueOf(dist.intValue());
                            gr = 255;
                            b = 0;
                        } else {
                            ut = ut - 0.5;
                            rd = 255;
                            dist = Math.round(255 - (255 * ut * 2));
                            gr = Integer.valueOf(dist.intValue());
                            b = 0;
                        }
                        
                        
                        Color cl = new Color(rd, gr, b);
                        g2.setColor(cl);
                    }
                    
                    g2.drawLine(last_x[c], h - 51 - (c) * y_step, xs[c] + i * x_step, h - 51 - (c) * y_step);
                    int max = Math.max(1, y_step - 1);
                    for (int width = 0; width < max; width++) {
                        g2.drawLine(last_x[c], h - 51 - (c) * y_step - width, xs[c] + i * x_step, h - 51 - (c) * y_step - width);
                    }
                    //g2.setColor(Color.white);
                    //g2.drawString(cl_util.get(cl_pointer)+"",last_x[c], h - 50 - (c) * y_step);
                    
                    last_x[c] = xs[c] + i * x_step;
                    cl_pointer++;
                }
            }
            
            y_count = 100;
            y_step = Math.max(1, (Math.round(((h - 70)) / y_count)));
            
            for (int ff = 0; ff < 50; ff++) {
                Long dist = Math.round(255 * (ff / 100.0) * 2);
                int rd = Integer.valueOf(dist.intValue());
                int gr = 255;
                int b = 0;
                Color cl = new Color(rd, gr, b);
                g2.setColor(cl);
                int max = Math.max(1, y_step - 1);
                for (int width = 0; width <= max; width++) {
                    g2.drawLine(w - 34, h - 52 - (y_step * ff) - width, w - 43, h - 52 - (y_step * ff) - width);
                }
                if (ff % 25 == 0) {
                    g2.drawString(ff + "%", w - 32, h - 50 - (y_step * ff));
                }
            }
            
            for (int ff = 50; ff < 101; ff++) {
                Long dist = Math.round(255 * ((ff - 50) / 100.0) * 2);
                int rd = 255;
                int gr = 255 - Integer.valueOf(dist.intValue());
                int b = 0;
                Color cl = new Color(rd, gr, b);
                g2.setColor(cl);
                int max = Math.max(1, y_step - 1);
                for (int width = 0; width <= max; width++) {
                    g2.drawLine(w - 34, h - 52 - (y_step * ff) - width, w - 43, h - 52 - (y_step * ff) - width);
                }
                if (ff % 25 == 0) {
                    g2.drawString(ff + "%", w - 32, h - 50 - (y_step * ff));
                }
            }
            Color cl = new Color(68, 68, 68);
            g2.setColor(cl);
            int max = Math.max(1, y_step - 1);
            for (int width = 0; width <= max * 6; width++) {
                g2.drawLine(w - 34, h - 10 - width, w - 43, h - 10 - width);
            }
            
            g2.drawString("down", w - 32, h - 10);
            
            
            /*for (int c = 0; c < cl_count; c++) {
            g2.setColor(colors[c]);
            int sub = Math.min(cl_names.get(c).length(), 20);
            String name = cl_names.get(c).substring(0, sub);
            g2.drawString(name + "...", w - 150, 20 + 13 * c);
            //g2.setColor(colors[c]);
            //g2.drawLine(w - 20, 5 + 5 * c, w - 10, 5 + 5 * c);
            }*/
            
            this.cl_usage_col = true;
        }
        if (cl_status_col) {
            //draw y axis
            g2.drawLine(150, 20, 150, h - 50);
            int y_count = cl_count;
            int y_step = Math.max(1, (Math.round(((h - 70)) / y_count)));
            for (int c = 0; c < cl_count; c++) {
                //g2.setColor(colors[c]);
                int sub = Math.min(cl_names.get(c).length(), 20);
                String name = cl_names.get(c).substring(0, sub);
                g2.drawString(name + "...", 5, h - 50 - (c) * y_step);
                //g2.setColor(colors[c]);
                //g2.drawLine(w - 20, 5 + 5 * c, w - 10, 5 + 5 * c);
            }
            
            g2.drawLine(150, h - 50, 150, h - 44);
            int y_point = 0;
            int y_num = 0;
            
            if (y_step >= 20) {
                y_point = 1;
                y_num = 1;
            } else {
                y_point = 2;
                y_num = 5;
            }
            
            g2.setColor(Color.black);
            
            
            
            // draw x axis
            g2.drawLine(150, h - 50, w - 50, h - 50);
            int x_count = days.size();
            int x_step = Math.max(1, (Math.round(((w - 200)) / x_count)));
            int x = 1;
            g2.drawString("0", 147, h - 25);
            g2.drawLine(150, h - 50, 150, h - 44);
            int x_point = 0;
            int x_num = 0;
            if (x_step >= 30) {
                x_point = 1;
                x_num = 1;
            } else if (x_step < 30 && x_step >= 10) {
                x_point = 1;
                x_num = 5;
            } else if (x_step < 10 && x_step >= 5) {
                x_point = 5;
                x_num = 10;
            } else {
                x_point = 5;
                x_num = 25;
            }
            while ((x * x_step) + 190 <= (w - 10)) {
                int xs = (x * x_step) + 150;
                if (x % x_point == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 47);
                }
                if (x % x_num == 0) {
                    g2.drawLine(xs, h - 50, xs, h - 44);
                    int factor = 3;
                    if (x < 10) {
                        factor = 3;
                    } else if (x < 100) {
                        factor = 5;
                    } else if (x >= 100) {
                        factor = 8;
                    }
                    g2.drawString("" + x, xs - factor, h - 25);
                }
                x++;
            }
            
            //g2.drawString(x_step+" = step: "+x +"%"+ x_num+" = "+x % x_num, w / 2 - 70, 15);
            g2.drawString("Percentage of failed CPUs per day [%]", w / 2 - 70, 15);
            g2.drawString("days", w / 2 - 10, h - 5);
            int cl_pointer = 0;
            int xs[] = new int[cl_count];
            int last_x[] = new int[cl_count];
            for (int c = 0; c < cl_count; c++) {
                xs[c] = 151;
                last_x[c] = 151;
            }
            for (int i = 1; i <= days.size(); i++) {
                for (int c = 0; c < cl_count; c++) {
                    
                    double ut = cl_status.get(cl_pointer);
                    //ut = ut / 100.0;
                    int rd = 0;
                    int gr = 0;
                    int b = 0;
                    
                    Long dist = 0l;
                                        
                    dist = Math.round(2 * ut);
                    rd = Integer.valueOf(dist.intValue());
                    gr = Integer.valueOf(dist.intValue());
                    b = Integer.valueOf(dist.intValue());
                    Color cl = new Color(rd, gr, b);
                    
                    g2.setColor(cl);
                    
                    g2.drawLine(last_x[c], h - 51 - (c) * y_step, xs[c] + i * x_step, h - 51 - (c) * y_step);
                    int max = Math.max(1, y_step - 1);
                    for (int width = 0; width < max; width++) {
                        g2.drawLine(last_x[c], h - 51 - (c) * y_step - width, xs[c] + i * x_step, h - 51 - (c) * y_step - width);
                    }
                    //g2.setColor(Color.white);
                    //g2.drawString(cl_util.get(cl_pointer)+"",last_x[c], h - 50 - (c) * y_step);
                    
                    last_x[c] = xs[c] + i * x_step;
                    cl_pointer++;
                }
            }
            
            y_count = 100;
            y_step = Math.max(1, (Math.round(((h - 70)) / y_count)));
            
            for (int ff = 0; ff < 101; ff++) {
                
                int rd = 200 - ff*2;
                int gr = 200 - ff*2;
                int b = 200 - ff*2;
                Color cl = new Color(rd, gr, b);
                g2.setColor(cl);
                int max = Math.max(1, y_step - 1);
                for (int width = 0; width <= max; width++) {
                    g2.drawLine(w - 34, h - 52 - (y_step * ff) - width, w - 43, h - 52 - (y_step * ff) - width);
                }
                if (ff % 25 == 0) {
                    g2.drawString(ff + "%", w - 32, h - 50 - (y_step * ff));
                }
            }
            
            this.cl_status_col = true;
        }
    }
    
    /**
     * run thread
     */
    public void run() {
        repaint();
    }
    
    
    /**
     * redraw certain graph based on input data
     */
    public void reDrawUsage(LinkedList days, LinkedList util, long timeS) {
        this.usage = true;
        this.time = timeS;
        this.days = days;
        this.util = util;
        repaint();
    }
    
    /**
     * redraw certain graph based on input data
     */
    public void reDrawDayUsage(LinkedList hours, LinkedList day_util, long timeS) {
        this.day_usage = true;
        this.time = timeS;
        this.hours = hours;
        this.day_util = day_util;
        repaint();
    }
    
    /**
     * redraw certain graph based on input data
     */
    public void reDrawClusterUsage(LinkedList days, LinkedList cl_util, long timeS, int cl_count, LinkedList cl_names) {
        this.cl_usage = true;
        this.time = timeS;
        this.days = days;
        this.cl_util = cl_util;
        this.cl_names = cl_names;
        this.cl_count = Math.min(cl_util.size(), cl_count);
        repaint();
    }
    
    /**
     * redraw certain graph based on input data
     */
    public void reDrawClusterUsageCol(LinkedList days, LinkedList cl_util, long timeS, int cl_count, LinkedList cl_names) {
        this.cl_usage_col = true;
        this.time = timeS;
        this.days = days;
        this.cl_util = cl_util;
        this.cl_names = cl_names;
        this.cl_count = Math.min(cl_util.size(), cl_count);
        repaint();
    }
    
    /**
     * redraw certain graph based on input data
     */
    public void reDrawClusterStatusCol(LinkedList days, LinkedList cl_status, long timeS, int cl_count, LinkedList cl_names) {
        this.cl_status_col = true;
        this.time = timeS;
        this.days = days;
        this.cl_status = cl_status;
        this.cl_names = cl_names;
        this.cl_count = Math.min(cl_status.size(), cl_count);
        repaint();
    }
    
    /**
     * redraw certain graph based on input data
     */
    public void reDrawDayClusterUsage(LinkedList days, LinkedList cl_util_h, long timeS, int cl_count, LinkedList cl_names) {
        this.dcl_usage = true;
        this.time = timeS;
        this.days = days;
        this.cl_util_h = cl_util_h;
        this.cl_names = cl_names;
        this.cl_count = Math.min(cl_util_h.size(), cl_count);
        repaint();
    }
    
    /**
     * redraw certain graph based on input data.
     */
    public void reDrawRW(LinkedList days, LinkedList waiting, LinkedList running, long timeS) {
        this.rw = true;
        this.days = days;
        this.time = timeS;
        this.waiting = waiting;
        this.running = running;
        repaint();
    }
    
    /**
     * redraw certain graph based on input data.
     */
    public void reDrawRU(LinkedList days, LinkedList requested, LinkedList used, long timeS, int totCPUs, LinkedList availCPUs) {
        this.ru = true;
        this.days = days;
        this.time = timeS;
        this.requested = requested;
        this.used = used;
        this.totCPUs = totCPUs;
        this.availCPUs = availCPUs;
        repaint();
    }
    
    /**
     * start thread.
     */
    public void start() {
        if (!animating) {
            animating = true;
            thread = new Thread(this);
            thread.setPriority(Thread.NORM_PRIORITY);
            thread.start();
        }
    }
    
    /**
     * stop thread.
     */
    public void stop() {
        animating = false;
        if (thread != null) {
            thread.interrupt();
        }
        thread = null;
    }
    
    /**
     * generates bitmap image.
     */
    private BufferedImage makeImage() {
        int w = getWidth();
        int h = getHeight();
        int type = BufferedImage.TYPE_INT_RGB;
        BufferedImage imagew = new BufferedImage(w, h, type);
        Graphics2D g2 = imagew.createGraphics();
        paint(g2);
        g2.dispose();
        return imagew;
    }
    
    
    /**
     * saves bitmap image into file.
     */
    private void save(BufferedImage image, String i, String ext, String identifier) {
        System.out.println("saving file to: " + ext);
        File file = new File("graphs/"+identifier+"-"+ i +"." + ext);
        try {
            ImageIO.write(image, ext, file);
        } catch (IOException e) {
            System.out.println("write error: " + e.getMessage());
        }
    }
    
    /**
     * method called by the MainFrame instance. This method will call save method inside this class.
     */
    public void saveToFile3(String ext, String identifier) {
        save(makeImage(), getName(), ext, identifier);
    }
}