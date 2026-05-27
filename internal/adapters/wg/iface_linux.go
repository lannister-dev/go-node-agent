//go:build linux

package wg

import (
	"errors"
	"fmt"
	"net"

	"github.com/vishvananda/netlink"
)

func ensureInterface(name string, addr net.IP, mask net.IPMask) error {
	link, err := netlink.LinkByName(name)
	if err != nil {
		var notFound netlink.LinkNotFoundError
		if !errors.As(err, &notFound) {
			return fmt.Errorf("wg: lookup %s: %w", name, err)
		}
		la := netlink.NewLinkAttrs()
		la.Name = name
		wg := &netlink.GenericLink{LinkAttrs: la, LinkType: "wireguard"}
		if err := netlink.LinkAdd(wg); err != nil {
			return fmt.Errorf("wg: add %s: %w", name, err)
		}
		link, err = netlink.LinkByName(name)
		if err != nil {
			return fmt.Errorf("wg: lookup after add %s: %w", name, err)
		}
	}
	desired := &netlink.Addr{IPNet: &net.IPNet{IP: addr, Mask: mask}}
	addrs, err := netlink.AddrList(link, netlink.FAMILY_V4)
	if err != nil {
		return fmt.Errorf("wg: addr list %s: %w", name, err)
	}
	have := false
	for _, a := range addrs {
		if a.IPNet != nil && a.IP.Equal(desired.IP) {
			have = true
			continue
		}
		_ = netlink.AddrDel(link, &a)
	}
	if !have {
		if err := netlink.AddrAdd(link, desired); err != nil {
			return fmt.Errorf("wg: addr add %s: %w", name, err)
		}
	}
	if link.Attrs().Flags&net.FlagUp == 0 {
		if err := netlink.LinkSetUp(link); err != nil {
			return fmt.Errorf("wg: link up %s: %w", name, err)
		}
	}
	return nil
}
