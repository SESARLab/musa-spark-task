package it.unimi.evotion.tasks.util;

public class FRatioPValue {

    public static double eval(double f, long n1, long n2) {
        return f(f, n1, n2);
    }

    static final int MAXPREC = 8;

    static class Gamma {
        static final double MACHEPS = 0.000001;

        public static double logGamma(double y) {
            final double PNT68 = 0.6796875D;

            /*	Largest double value for which log(gamma(x)) can be represented.
             */

            final double XBIG = 2.5E305;

            /*	Approximate fourth root of XBIG.
             */

            final double FRTBIG = 2.25E76;

            /*	Numerator and denominator coefficients for rational minimax
             *	approximation over (0.5,1.5).
             */
            final double LNSQRT2PI = Math.log(Math.sqrt(2.0D * Math.PI));

            final double d1 = -5.772156649015328605195174E-1;

            final double[] p1 =
                {
                    4.945235359296727046734888E0,
                    2.018112620856775083915565E2,
                    2.290838373831346393026739E3,
                    1.131967205903380828685045E4,
                    2.855724635671635335736389E4,
                    3.848496228443793359990269E4,
                    2.637748787624195437963534E4,
                    7.225813979700288197698961E3
                };

            final double[] q1 =
                {
                    6.748212550303777196073036E1,
                    1.113332393857199323513008E3,
                    7.738757056935398733233834E3,
                    2.763987074403340708898585E4,
                    5.499310206226157329794414E4,
                    6.161122180066002127833352E4,
                    3.635127591501940507276287E4,
                    8.785536302431013170870835E3
                };

            /*	Numerator and denominator coefficients for rational minimax
             *	Approximation over (1.5,4.0).
             */

            final double d2 = 4.227843350984671393993777E-1;

            final double[] p2 =
                {
                    4.974607845568932035012064E0,
                    5.424138599891070494101986E2,
                    1.550693864978364947665077E4,
                    1.847932904445632425417223E5,
                    1.088204769468828767498470E6,
                    3.338152967987029735917223E6,
                    5.106661678927352456275255E6,
                    3.074109054850539556250927E6
                };

            final double[] q2 =
                {
                    1.830328399370592604055942E2,
                    7.765049321445005871323047E3,
                    1.331903827966074194402448E5,
                    1.136705821321969608938755E6,
                    5.267964117437946917577538E6,
                    1.346701454311101692290052E7,
                    1.782736530353274213975932E7,
                    9.533095591844353613395747E6
                };

            /*	Numerator and denominator coefficients for rational minimax
             *	Approximation over (4.0,12.0).
             */

            final double d4 = 1.791759469228055000094023D;

            final double[] p4 =
                {
                    1.474502166059939948905062E4,
                    2.426813369486704502836312E6,
                    1.214755574045093227939592E8,
                    2.663432449630976949898078E9,
                    2.940378956634553899906876E10,
                    1.702665737765398868392998E11,
                    4.926125793377430887588120E11,
                    5.606251856223951465078242E11
                };

            final double[] q4 =
                {
                    2.690530175870899333379843E3,
                    6.393885654300092398984238E5,
                    4.135599930241388052042842E7,
                    1.120872109616147941376570E9,
                    1.488613728678813811542398E10,
                    1.016803586272438228077304E11,
                    3.417476345507377132798597E11,
                    4.463158187419713286462081E11
                };

            /*	Coefficients for minimax approximation over (12, INF).
             */

            final double[] c =
                {
                    -1.910444077728E-03,
                    8.4171387781295E-04,
                    -5.952379913043012E-04,
                    7.93650793500350248E-04,
                    -2.777777777777681622553E-03,
                    8.333333333333333331554247E-02,
                    5.7083835261E-03
                };

            double res;
            double corr;
            double ysq;
            double xden;
            double xnum;
            double xm1;
            double xm2;
            double xm4;
            //	If argument is not a number,
            //	return that.

            if (Double.isNaN(y)) {
                return y;
            }

            if ((y > 0.0) && (y <= XBIG)) {
                if (y <= MACHEPS) {
                    res = -Math.log(y);
                } else if (y <= 1.5D) {
                    // MACHEPS < x <= 1.5

                    if (y < PNT68) {
                        corr = -Math.log(y);
                        xm1 = y;
                    } else {
                        corr = 0.0D;
                        xm1 = (y - 0.5D) - 0.5D;
                    }

                    if ((y <= 0.5D) || (y >= PNT68)) {
                        xden = 1.0D;
                        xnum = 0.0D;

                        for (int i = 0; i < 8; i++) {
                            xnum = xnum * xm1 + p1[i];
                            xden = xden * xm1 + q1[i];
                        }

                        res = corr + (xm1 * (d1 + xm1 * (xnum / xden)));
                    } else {
                        xm2 = (y - 0.5D) - 0.5D;
                        xden = 1.0D;
                        xnum = 0.0D;

                        for (int i = 0; i < 8; i++) {
                            xnum = xnum * xm2 + p2[i];
                            xden = xden * xm2 + q2[i];
                        }

                        res = corr + xm1 * (d2 + xm2 * (xnum / xden));
                    }
                } else if (y <= 4.0D) {
                    // .5 .LT. X .LE. 4.0

                    xm2 = y - 2.0D;
                    xden = 1.0D;
                    xnum = 0.0D;

                    for (int i = 0; i < 8; i++) {
                        xnum = xnum * xm2 + p2[i];
                        xden = xden * xm2 + q2[i];
                    }

                    res = xm2 * (d2 + xm2 * (xnum / xden));
                } else if (y <= 12.0D) {
                    //	4.0 < x < 12.0

                    xm4 = y - 4.0D;
                    xden = -1.0D;
                    xnum = 0.0D;

                    for (int i = 0; i < 8; i++) {
                        xnum = xnum * xm4 + p4[i];
                        xden = xden * xm4 + q4[i];
                    }

                    res = d4 + xm4 * (xnum / xden);
                } else {
                    //	Evaluate for argument .GE. 12.0
                    res = 0.0D;

                    if (y <= FRTBIG) {
                        res = c[6];

                        ysq = y * y;

                        for (int i = 0; i < 6; i++) {
                            res = res / ysq + c[i];
                        }
                    }

                    res = res / y;
                    corr = Math.log(y);

                    res = res + LNSQRT2PI - 0.5D * corr;
                    res = res + y * (corr - 1.0D);
                }
            } else {
                // Return largest possible positive value
                // for bad arguments.

                res = Double.POSITIVE_INFINITY;
//			res = Double.MAX_VALUE;
            }

            return res;
        }

    }

    static class Beta {

        /**
         * Cumulative probability density function for the incomplete beta function.
         *
         * @param x     Upper percentage point of incomplete beta
         *              probability density function
         * @param alpha First shape parameter
         * @param beta  Second shape parameter
         * @param dPrec Digits of precision desired (1 < dPrec < MAXPREC)
         * @return Cumulative probability density function value.
         * @throws IllegalArgumentException if x <= 0 or a <= 0 or b <= 0 .
         *
         *                                  <p>
         *                                  The continued fraction expansion as given by
         *                                  Abramowitz and Stegun (1964) is used.  This
         *                                  method works well unless the minimum of (alpha, beta)
         *                                  exceeds about 70000.  For most common values the result
         *                                  will be accurate to about 14 decimal digits.
         *                                  </p>
         */

        static double incompleteBeta
        (
            double x,
            double alpha,
            double beta,
            int dPrec
        )
            throws IllegalArgumentException {
            double epsz;
            double a;
            double b;
            double c;
            double f;
            double fx;
            double apb;
            double zm;
            double alo;
            double ahi;
            double blo;
            double bhi;
            double bod;
            double bev;
            double zm1;
            double d1;
            double aev;
            double aod;
            double cPrec;
            double result;

            int nTries;
            int iter;
            int maxIter;

            boolean qSwap;
            boolean qDoit;
            boolean qConv;

            /* Initialize */

            if (dPrec > MAXPREC) {
                dPrec = MAXPREC;
            } else if (dPrec <= 0) {
                dPrec = 1;
            }

            cPrec = dPrec;

            epsz = Math.pow(10.0D, -dPrec);

            a = alpha;
            b = beta;
            qSwap = false;
            result = -1.0D;
            qDoit = true;
            maxIter = 200;
            /* Check arguments */
            /* Error if:       */
            /*    X <= 0       */
            /*    A <= 0       */
            /*    B <= 0       */
            if (x <= 0.0D) {
                throw new IllegalArgumentException("x <= 0.0");
            }

            if (a <= 0.0D) {
                throw new IllegalArgumentException("a <= 0.0");
            }

            if (b <= 0.0D) {
                throw new IllegalArgumentException("b <= 0.0");
            }

            result = 1.0D;
            /* If X >= 1, return 1.0 as prob */

            if (x >= 1.0D) return result;

            /* If x > a / ( a + b ) then swap */
            /* a, b for more efficient eval.  */

            if (x > (a / (a + b))) {
                x = 1.0 - x;
                a = beta;
                b = alpha;
                qSwap = true;
            }
            ;

            /* Check for extreme values */

            if ((x == a) || (x == b)) {
            } else if (a == ((b * x) / (1.0 - x))) {
            } else if (Math.abs(a - (x * (a + b))) <= epsz) {
            } else {
                c =
                    Gamma.logGamma(a + b) + a * Math.log(x) +
                        b * Math.log(1.0 - x) - Gamma.logGamma(a) -
                        Gamma.logGamma(b) - Math.log(a - x * (a + b));

                if ((c < -36.0D) && qSwap) return result;

                result = 0.0D;

                if (c < -180.0D) return result;
            }
            /*  Set up continued fraction expansion */
            /*  evaluation.                         */
            apb = a + b;
            zm = 0.0D;
            alo = 0.0D;
            bod = 1.0D;
            bev = 1.0D;
            bhi = 1.0D;
            blo = 1.0D;

            ahi =
                Math.exp(
                    Gamma.logGamma(apb) + a * Math.log(x) +
                        b * Math.log(1.0D - x) - Gamma.logGamma(a + 1.0D) -
                        Gamma.logGamma(b));

            f = ahi;
            iter = 0;
            /* Continued fraction loop begins here. */
            /* Evaluation continues until maximum   */
            /* iterations are exceeded, or          */
            /* convergence achieved.                */
            qConv = false;

            do {
                fx = f;

                zm1 = zm;
                zm = zm + 1.0D;
                d1 = a + zm + zm1;
                aev = -(a + zm1) * (apb + zm1) * x / d1 / (d1 - 1.0D);
                aod = zm * (b - zm) * x / d1 / (d1 + 1.0D);
                alo = bev * ahi + aev * alo;
                blo = bev * bhi + aev * blo;
                ahi = bod * alo + aod * ahi;
                bhi = bod * blo + aod * bhi;

                if (Math.abs(bhi) < Double.MIN_VALUE) bhi = 0.0D;

                if (bhi != 0.0D) {
                    f = ahi / bhi;
                    qConv = (Math.abs((f - fx) / f) < epsz);
                }
                ;

                iter++;
            }
            while ((iter <= maxIter) && (!qConv));

            /* Arrive here when convergence    */
            /* achieved, or maximum iterations */
            /* exceeded.                       */
            if (qSwap) {
                result = 1.0D - f;
            } else {
                result = f;
            }

            return result;
        }

    }

    static double f(double f, double dfn, double dfd) {
        double result = 0.0D;

        result =
            Beta.incompleteBeta
                (
                    dfd / (dfd + f * dfn),
                    dfd / 2.0D,
                    dfn / 2.0D,
                    MAXPREC
                );

        return result;
    }

}
