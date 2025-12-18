#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/printk.h>
#include <linux/proc_fs.h>
#include <linux/uaccess.h>
#include <linux/version.h>
#include <linux/smp.h> // Для num_online_cpus()
#include <linux/string.h>

#if LINUX_VERSION_CODE >= KERNEL_VERSION(5, 6, 0)
#define HAVE_PROC_OPS
#endif

#define PROCFS_MAX_SIZE 1024
#define PROCFS_NAME "lab3_os"

static struct proc_dir_entry *our_proc_file;
static char procfs_buffer[PROCFS_MAX_SIZE];
static unsigned long procfs_buffer_size = 0;

static ssize_t procfile_read(struct file *file_pointer, char __user *buffer, size_t buffer_length, loff_t *offset)
{
    char cpu_info[128];
    int len;
    ssize_t ret;
    
    unsigned int num_cpus = num_online_cpus();
    
    len = snprintf(cpu_info, sizeof(cpu_info), "Cores: %u.\nYou need: %u.\n", num_cpus, num_cpus < 16 ? 16 - num_cpus : 0);
    
    if (*offset >= len) {
        ret = 0;
    } else {
        if (copy_to_user(buffer, cpu_info + *offset, len - *offset)) {
            pr_info("Ошибка при выполнении copy_to_user\n");
            ret = -EFAULT;
        } else {
            ret = len - *offset;
            *offset += ret;
        }
    }
    return ret;
}

static ssize_t procfile_write(struct file *file, const char __user *buff, size_t len, loff_t *off)
{
    procfs_buffer_size = len;
    if (procfs_buffer_size >= PROCFS_MAX_SIZE)
        procfs_buffer_size = PROCFS_MAX_SIZE - 1;

    if (copy_from_user(procfs_buffer, buff, procfs_buffer_size))
        return -EFAULT;

    procfs_buffer[procfs_buffer_size] = '\0';
    *off += procfs_buffer_size;
    pr_info("Запись в procfile %s\n", procfs_buffer);
    return procfs_buffer_size;
}

#ifdef HAVE_PROC_OPS
static const struct proc_ops proc_file_fops = {
    .proc_read = procfile_read,
    .proc_write = procfile_write,
};
#else
static const struct file_operations proc_file_fops = {
    .read = procfile_read,
    .write = procfile_write,
};
#endif

static int __init procfs_init(void)
{
    pr_info("Welcome to the Tomsk State University\n");
    
    our_proc_file = proc_create(PROCFS_NAME, 0644, NULL, &proc_file_fops);
    if (NULL == our_proc_file) {
        pr_alert("Ошибка: Не удалось создать /proc/%s\n", PROCFS_NAME);
        return -ENOMEM;
    }
    
    pr_info("Создан /proc/%s\n", PROCFS_NAME);
    return 0;
}

static void __exit procfs_exit(void)
{
    pr_info("Tomsk State University forever!\n");
    proc_remove(our_proc_file);
    pr_info("/proc/%s удален\n", PROCFS_NAME);
}

module_init(procfs_init);
module_exit(procfs_exit);
MODULE_LICENSE("GPL");