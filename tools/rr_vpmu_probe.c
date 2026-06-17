/* rr_vpmu_probe.c -- diagnose whether this (virtualized) PMU can run rr.
 *
 *   cc -O2 -o /tmp/rr_vpmu_probe tools/rr_vpmu_probe.c && /tmp/rr_vpmu_probe
 *
 * rr needs a hardware retired-branch counter that supports (a) sampling and
 * (b) PERF_EVENT_IOC_PERIOD down to a SMALL period (rr's check uses period=1).
 * VMware's vPMU historically virtualizes counting only; partial configs allow
 * counting + sampling + large-period IOC_PERIOD but reject tiny periods.  This
 * probe reports the exact capability + the minimum accepted sample_period, so a
 * host-side vPMU change can be re-tested in one command.  See docs/dev/rr_vpmu_status.md.
 */
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <stdint.h>

static int open_ticks(uint64_t period) {
    struct perf_event_attr a;
    memset(&a, 0, sizeof a);
    a.type = PERF_TYPE_RAW;
    a.size = sizeof a;
    a.config = 0x5101c4;          /* BR_INST_RETIRED.CONDITIONAL (USR|OS) -- rr's ticks event */
    a.sample_period = period;
    a.exclude_kernel = 1;
    return syscall(__NR_perf_event_open, &a, 0, -1, -1, 0);   /* this thread, any cpu */
}
static int ioc_period_ok(uint64_t newp) {
    int fd = open_ticks(0xffffffffULL);
    if (fd < 0) return -1;
    int r = ioctl(fd, PERF_EVENT_IOC_PERIOD, &newp);
    close(fd);
    return r == 0;
}

int main(void) {
    int fd = open_ticks(0xffffffffULL);
    if (fd < 0) { printf("FAIL: perf_event_open(retired-branches, sampling) -> %s\n"
                         "      vPMU exposes no usable sampling counter.\n", strerror(errno)); return 2; }
    close(fd);
    printf("OK: sampling retired-branch counter opens.\n");

    if (ioc_period_ok(1)) {
        printf("OK: IOC_PERIOD(period=1) accepted -- rr's check_for_ioc_period_bug WILL PASS.\n");
        printf("==> rr should record. Try: rr record -n /bin/true\n");
        return 0;
    }
    /* period=1 rejected: find the minimum accepted period. */
    uint64_t lo = 1, hi = 0xffffffff;
    while (lo + 1 < hi) { uint64_t m = lo + (hi - lo) / 2; if (ioc_period_ok(m)) hi = m; else lo = m; }
    printf("vPMU REJECTS small sample_period: min accepted = %lu (period %lu = EINVAL).\n",
           (unsigned long)hi, (unsigned long)lo);
    printf("==> rr's check_for_ioc_period_bug uses period=1 -> it ABORTS here.\n");
    printf("    Fix paths: (1) a host vPMU setting that allows period<%lu, or\n", (unsigned long)hi);
    printf("    (2) a min-period-clamped rr build (clamp reset()+the check to >=%lu).\n", (unsigned long)hi);
    return 1;
}
