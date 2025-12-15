#include<linux/kernel.h>
#include <linux/module.h>
#include <linux/printk.h>
#include<linux/proc_fs.h>
#include<linux/uaccess.h>
#include <linux/version.h>

#define procfs_name "tsu"

int __init init_mod(void)
{
    pr_info("Welcome to the Tomsk State University\n");
    return 0;
}

void __exit cleanup_mod(void)
{
    pr_info("Tomsk State University forever\n");
}

module_init(init_mod);
module_exit(cleanup_mod);

MODULE_LICENSE("GPL");