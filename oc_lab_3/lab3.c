#include<linux/kernel.h>
#include <linux/module.h>
#include <linux/printk.h>
#include<linux/proc_fs.h>
#include<linux/uaccess.h>
#include <linux/version.h>

#define procfs_name "tsu"

static int __init module_init(void)
{
    pr_info("Welcome to the Tomsk State University\n");
    return 0;
}

static void __exit module_cleanup(void)
{
    pr_info("Tomsk State University forever\n");
}

module_init(module_init);
module_exit(module_cleanup);

MODULE_LICENSE("GPL");